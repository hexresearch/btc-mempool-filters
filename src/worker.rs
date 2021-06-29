use std::sync::Arc;
use bitcoin::consensus::encode;
use bitcoin::network::message::NetworkMessage;
use futures::Future;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::error::MempoolErrors;

use crate::txtree::TxTree;
use futures::sink;
use bitcoin::network::message_blockdata::Inventory;

pub async fn sync_mempool(
    txtree: Arc<Mutex<TxTree>>,
) -> (
    impl Future<Output = Result<(), MempoolErrors>>,
    impl futures::Sink<NetworkMessage, Error = encode::Error>,
    impl Unpin + futures::Stream<Item = NetworkMessage>
)
{
    const BUFFER_SIZE: usize = 100;
    let (broad_sender, _) = broadcast::channel(BUFFER_SIZE);
    let (msg_sender, msg_reciver) = mpsc::unbounded_channel::<NetworkMessage>();
    let sync_future = {
        let broad_sender = broad_sender.clone();
        async move {
            loop {
                mempool_worker(txtree.clone(), &broad_sender, &msg_sender).await?;
            }
        }
    };
    let msg_stream = UnboundedReceiverStream::new(msg_reciver);
    let msg_sink = sink::unfold(broad_sender, |broad_sender, msg| async move {
        broad_sender.send(msg).unwrap_or(0);
        Ok::<_, encode::Error>(broad_sender)
    });
    (sync_future, msg_sink, msg_stream)
}

pub async fn mempool_worker(
    txtree: Arc<Mutex<TxTree>>,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
) -> Result<(), MempoolErrors>{
    let mut receiver = broad_sender.subscribe();
    loop {
        tokio::select! {
            emsg = receiver.recv() => match emsg {
                Ok(msg) => match msg {
                    NetworkMessage::Inv(ids) => {
                        let txids : Vec<Inventory>= ids.iter().filter(|i| match i {
                            Inventory::Transaction(_) => true,
                            Inventory::WitnessTransaction(_) => true,
                            _ => false
                        }).cloned().collect();
                        println!("Received {:?} invs. Of which {:?} are txs", ids.len(), txids.len());
                        msg_sender.send(NetworkMessage::GetData(txids))
                            .map_err(|e| { println!("Error when requesting txs: {:?}", e); MempoolErrors::MempoolRequestFail})?;
                    },
                    NetworkMessage::Tx(tx) => {
                        let mut txtree = txtree.lock().await;
                        txtree.insert_single(&tx);
                    }
                    _ => ()
                },
                Err(e) => eprintln!("mempool_worker: {:?}", e),
            }
        }
    };
}

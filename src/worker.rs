use bitcoin_utxo::cache::utxo::UtxoCache;
use bitcoin_utxo::cache::utxo::wait_utxo_noh;
use bitcoin::consensus::Decodable;
use bitcoin::consensus::encode;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message::NetworkMessage;
use bitcoin::OutPoint;
use bitcoin::Script;
use bitcoin::util::bip158;
use ergvein_filters::mempool::ErgveinMempoolFilter;
use futures::Future;
use futures::sink;
use rocksdb::DB;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::error::MempoolErrors;
use crate::filtertree::FilterTree;
use crate::filtertree::make_filters;
use crate::filtertree::make_full_filter;
use crate::txtree::insert_tx;
use crate::txtree::TxTree;

pub async fn mempool_worker<T, M>(
    txtree: Arc<TxTree>,
    ftree: Arc<FilterTree>,
    full_filter: Arc<Mutex<Option<ErgveinMempoolFilter>>>,
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    script_from_t : M,
) -> (
    impl Future<Output = Result<(), MempoolErrors>>,
    impl Future<Output = Result<(), MempoolErrors>>,
    impl futures::Sink<NetworkMessage, Error = encode::Error>,
    impl Unpin + futures::Stream<Item = NetworkMessage>
)
where
T:Decodable + Clone,
M:Fn(&T) -> Script + Copy
{
    const BUFFER_SIZE: usize = 100;
    let (broad_sender, _) = broadcast::channel(BUFFER_SIZE);
    let (msg_sender, msg_reciver) = mpsc::unbounded_channel::<NetworkMessage>();
    let tx_future = {
        let broad_sender = broad_sender.clone();
        let txtree = txtree.clone();
        async move {
            loop {
                tx_listener(txtree.clone(), &broad_sender, &msg_sender).await?;
            }
        }
    };
    let filt_future = filter_worker(
            txtree.clone(),
            ftree.clone(),
            full_filter.clone(),
            db.clone(),
            cache.clone(),
            script_from_t);

    let msg_stream = UnboundedReceiverStream::new(msg_reciver);
    let msg_sink = sink::unfold(broad_sender, |broad_sender, msg| async move {
        broad_sender.send(msg).unwrap_or(0);
        Ok::<_, encode::Error>(broad_sender)
    });
    (tx_future, filt_future, msg_sink, msg_stream)
}

pub async fn filter_worker<T, M>(
    txtree: Arc<TxTree>,
    ftree: Arc<FilterTree>,
    full_filter: Arc<Mutex<Option<ErgveinMempoolFilter>>>,
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    script_from_t : M,
) -> Result<(), MempoolErrors>
where
    T: Decodable + Clone,
    M: Fn(&T) -> Script,
{
    let mut hashmap = HashMap::<OutPoint, Script>::new();
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        hashmap.clear();
        for txs in txtree.iter() {
            for (_, tx) in txs.value().iter(){
                if !tx.is_coin_base(){
                    for i in &tx.input {
                        let coin = wait_utxo_noh(
                            db.clone(),
                            cache.clone(),
                            &i.previous_output,
                            Duration::from_millis(100),
                        ).await;
                        coin.map_or((), |t|{
                            hashmap.insert(i.previous_output, script_from_t(&t));
                        })
                    }
                }
            }
        };
        make_filters(&ftree, &txtree, |out| {
            hashmap.get(out).map_or( Err(bip158::Error::UtxoMissing(*out)), |s| Ok(s.clone()))
        });

        let ff = make_full_filter(&txtree, |out| {
            hashmap.get(out).map_or( Err(bip158::Error::UtxoMissing(*out)), |s| Ok(s.clone()))
        }).map_err(|e| eprintln!("Error making the full filter! {:?}", e)).ok();
        {
            let mut ffref = full_filter.lock().await;
            *ffref = ff;
            println!("Filter: {:?}", ffref);
        }
    };
}

pub async fn tx_listener(
    txtree: Arc<TxTree>,
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
                        let txtree = txtree.clone();
                        insert_tx(&txtree, &tx);
                    }
                    _ => ()
                },
                Err(e) => eprintln!("tx_listener: {:?}", e),
            }
        }
    };
}

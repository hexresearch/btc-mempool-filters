use bitcoin::{
    network::{message::NetworkMessage, message_blockdata::Inventory},
    Transaction, Txid,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc};

use crate::{
    error::MempoolErrors,
    txtree::TxTree,
    txtree::{insert_tx, remove_batch},
};

/// Sub-worker. Listen to incoming messages for the node
/// Pick all new transactions from `Inv` messages and request them
/// Get transactions and add the to the `TxTree`
pub async fn tx_listener(
    txtree: Arc<TxTree>,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
) -> Result<(), MempoolErrors> {
    let mut receiver = broad_sender.subscribe();
    loop {
        tokio::select! {
            emsg = receiver.recv() => match emsg {
                Ok(msg) => match msg {
                    NetworkMessage::Inv(ids) => {
                        let txids : Vec<Inventory>= ids.iter().filter(|i|
                            matches!(i, Inventory::Transaction(_) | Inventory::WitnessTransaction(_))
                        ).cloned().collect();
                        msg_sender.send(NetworkMessage::GetData(txids))
                            .map_err(|e| {
                                println!("Error when requesting txs: {:?}", e);
                                MempoolErrors::RequestTx
                            })?;
                    },
                    NetworkMessage::Tx(tx) => {
                        let txtree = txtree.clone();
                        insert_tx(&txtree, &tx);
                    }
                    NetworkMessage::Block(b) => {
                        let txtree = txtree.clone();
                        remove_batch(&txtree, &b.txdata);
                    }
                    _ => ()
                },
                Err(e) => eprintln!("tx_listener: {:?}", e),
            }
        }
    }
}

/// Request a `Transaction` from the node's mempool
/// Re-request every 5 seconds
pub async fn request_mempool_tx(
    txid: Txid,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
) -> Result<Transaction, MempoolErrors> {
    let mut receiver = broad_sender.subscribe();
    println!("Requesting from node. Tx: {:?}", txid);
    msg_sender
        .send(NetworkMessage::GetData(vec![Inventory::Transaction(txid)]))
        .map_err(|_| MempoolErrors::RequestTx)?;
    let timeout_future = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout_future);
    let mut res = None;
    while res == None {
        tokio::select! {
            _ = &mut timeout_future => {
                println!("Request for Tx: {} timed out", txid);
                break;
            }
            emsg = receiver.recv() => match emsg {
                Err(e) => {
                    eprintln!("Request tx {:?} failed with recv error: {:?}", txid, e);
                    msg_sender.send(NetworkMessage::GetData(vec![Inventory::Transaction(txid)])).map_err(
                        |_| MempoolErrors::RequestTx
                    )?;
                }
                Ok(msg) => match msg {
                    NetworkMessage::Tx(tx) if tx.txid() == txid => {
                        res = Some(tx);
                    }
                    _ => (),
                },
            }
        }
    }
    res.ok_or(MempoolErrors::RequestTx)
}

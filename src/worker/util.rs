use bitcoin::{
    consensus::Decodable, network::message::NetworkMessage, OutPoint, Script, Transaction,
};
use bitcoin_utxo::cache::utxo::{get_utxo_noh, UtxoCache};
use rocksdb::DB;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, mpsc};

use crate::{
    txtree::{get_transaction_script, TxTree},
    worker::request_mempool_tx,
};

/// Fill a hashmap with all input scripts for all txs in the txs tree (and more!)
/// Run fill_tx_input for all inputs
/// Add all extra transactions to extra stack and repeat the process untill the stack is empty
/// This is done because if there is a chain of transactions in the mempool and we request them during `fill_tx_input`
/// they get added to `TxTree` via `tx_listener` and we have to process their inputs also.
pub(crate) async fn fill_tx_map<T, M>(
    txtree: Arc<TxTree>,
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    hashmap: &mut HashMap<OutPoint, Script>,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
    script_from_t: M,
) where
    T: Decodable + Clone,
    M: Fn(&T) -> Script + Copy,
{
    let mut extra = Vec::new();
    let mut inputs = Vec::new();

    // Fill a separate structure to minimize the time we use txtree.iter
    txtree.iter().for_each(|txs|
        txs.values().for_each(|(tx,_)|
            tx.input.iter().for_each(|i|
                inputs.push(i.previous_output)
            )
        )
    );

    for i in inputs {
        let stx = fill_tx_input(
            &i,
            txtree.clone(),
            db.clone(),
            cache.clone(),
            hashmap,
            broad_sender,
            msg_sender,
            script_from_t,
        )
        .await;
        if let Some(tx) = stx {
            extra.push(tx)
        }
    }

    while !extra.is_empty() {
        let mut next_extra = Vec::new();
        for tx in extra.iter() {
            for i in tx.input.iter() {
                let stx = fill_tx_input(
                    &i.previous_output,
                    txtree.clone(),
                    db.clone(),
                    cache.clone(),
                    hashmap,
                    broad_sender,
                    msg_sender,
                    script_from_t,
                )
                .await;
                if let Some(tx) = stx {
                    next_extra.push(tx)
                }
            }
        }
        extra = next_extra;
    }
}

/// Get the script associated with the OutPoint i and add it to the hashmap
/// First check if it's in the utxo: cache, or the persistent storage
/// If not - assume it's in the mempool.
/// Check if we have the tx in the txtree
/// If it's not in the txtree, ask the node via `GetData`.
/// The node sends `Tx` message in response if the `Tx` in the mempool
/// If the tx was not present in cache or txtree, return the tx so that it can be filled later
async fn fill_tx_input<T, M>(
    i: &OutPoint,
    txtree: Arc<TxTree>,
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    hashmap: &mut HashMap<OutPoint, Script>,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
    script_from_t: M,
) -> Option<Transaction>
where
    T: Decodable + Clone,
    M: Fn(&T) -> Script + Copy,
{
    let mut res = None;
    let coin = get_utxo_noh(&db.clone(), &cache.clone(), &i);
    match coin {
        Some(c) => {
            hashmap.insert(*i, script_from_t(&c));
        }
        None => {
            let mscript = get_transaction_script(&txtree, i);
            match mscript {
                Some(script) => {
                    hashmap.insert(*i, script);
                }
                None => {
                    let stx = request_mempool_tx(i.txid, broad_sender, msg_sender).await;
                    match stx {
                        Ok(tx) => {
                            let script = tx.output[i.vout as usize].script_pubkey.clone();
                            hashmap.insert(*i, script);
                            res = Some(tx);
                        }
                        Err(_) => {
                            eprintln!("Failed to get Tx {:?} from mempool", i.txid);
                        }
                    }
                }
            }
        }
    }
    res
}

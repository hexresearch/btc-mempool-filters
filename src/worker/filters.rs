use bitcoin::{
    consensus::Decodable, network::message::NetworkMessage, util::bip158, OutPoint, Script,
};
use bitcoin_utxo::cache::utxo::UtxoCache;
use ergvein_filters::mempool::ErgveinMempoolFilter;
use futures::future::{AbortHandle, Abortable};
use rocksdb::DB;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::{
    error::MempoolErrors,
    filtertree::{make_filters, make_full_filter, FilterTree},
    txtree::{tx_tree_count, TxTree},
    worker::util::fill_tx_map,
};

/// Sub-worker. Builds filters based on the current mempool
/// Wait minimal period: filter_delay
/// Wait untill the utxo cache is fully synced (wait for `sync_mutex`)
/// Create a hashmap and fill input scripts from all inputs of all transactions in the mempool (in the `TxTree`)
/// If during the filling a new block is received, abort the attempt.
/// If the filling takes too long (`hashmap_timeout`) abort the attempt and yield the mutex.
/// This is done to prevent possible race between utxo sync and filters.
/// Just in case the timeout is not enough, increase it by 20% each time we timed out
/// After we filled the hashmap yield the mutex, run `make_filters` and `make_full_filter`
/// `script_from_t` is used to convert Coin representation from `T` in the `UtxoCache` to `Script`
pub async fn filter_worker<T, M>(
    txtree: Arc<TxTree>,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
    ftree: Arc<FilterTree>,
    full_filter: Arc<Mutex<Option<ErgveinMempoolFilter>>>,
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    sync_mutex: Arc<Mutex<()>>,
    script_from_t: M,
    filter_delay: Duration,
    hashmap_timeout: Duration,
) -> Result<(), MempoolErrors>
where
    T: Decodable + Clone,
    M: Fn(&T) -> Script + Copy,
{
    println!("[filter_worker]: Starting");
    let mut hashmap = HashMap::<OutPoint, Script>::new();
    let mut timeout = hashmap_timeout;
    let mut err_cnt = 0;
    let mut succ_cnt = 0;
    loop {
        tokio::time::sleep(filter_delay).await;
        {
            // Make sure the utxo cache is fully synced before attempting to make filters
            let _guard = sync_mutex.lock().await;
            hashmap.clear();
            let timeout_future = tokio::time::sleep(timeout);
            let new_block_future = wait_for_block(&broad_sender);
            let (timeout_handle, timeout_reg) = AbortHandle::new_pair();
            tokio::pin!(timeout_future);
            let fill_fut = Abortable::new(
                fill_tx_map(
                    txtree.clone(),
                    db.clone(),
                    cache.clone(),
                    &mut hashmap,
                    broad_sender,
                    msg_sender,
                    script_from_t,
                ),
                timeout_reg,
            );
            tokio::select! {
                _ = &mut timeout_future => {
                    println!("Hashmap filling timed out");
                    timeout_handle.abort();
                    // increase timeout for the next try by 1.25. Just in case the default timeout was not enough
                    timeout = timeout.mul_f64(1.25);
                }
                _ = new_block_future => {
                    println!("A new block interrupted filter making. Aborting filters!");
                    timeout_handle.abort();
                }
                _ = fill_fut => {
                    // set the timeout to the default value
                    timeout = hashmap_timeout;
                }
            };
        } // Unlock sync_mutex, sine we don't need the cache anymore

        make_filters(&ftree, &txtree, |out| {
            hashmap
                .get(out)
                .map_or_else(|| Err(bip158::Error::UtxoMissing(*out)), |s| Ok(s.clone()))
        });

        let ff = make_full_filter(&txtree, |out| {
            hashmap
                .get(out)
                .map_or_else(|| Err(bip158::Error::UtxoMissing(*out)), |s| Ok(s.clone()))
        })
        .map_err(|e| eprintln!("Error making the full filter! {:?}", e))
        .ok();
        {
            let mut ffref = full_filter.lock().await;
            *ffref = ff;
            let l = ffref.as_ref().map(|f| f.content.len()).unwrap_or(0);
            if ffref.is_none() {
                err_cnt += 1;
            } else {
                succ_cnt += 1;
            }
            println!(
                "Filter len: {:?} bytes. Txs in mempool: {}. Totals: Succ: {}. Error: {}",
                l,
                tx_tree_count(&txtree),
                succ_cnt,
                err_cnt
            );
        }
    }
}

/// Listens to incoming messages until it gets a `Block` message
/// Used to interrupt filter building
async fn wait_for_block(broad_sender: &broadcast::Sender<NetworkMessage>) {
    let mut receiver = broad_sender.subscribe();
    loop {
        let emsg = receiver.recv().await;
        match emsg {
            Err(_) => {}
            Ok(msg) => if let NetworkMessage::Block(_) = msg { break },
        }
    }
}

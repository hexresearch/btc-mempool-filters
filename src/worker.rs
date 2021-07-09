mod filters;
mod tx;
mod util;
use bitcoin::{
    consensus::{encode, Decodable},
    network::message::NetworkMessage,
    Script,
};
use bitcoin_utxo::cache::utxo::UtxoCache;
use ergvein_filters::mempool::ErgveinMempoolFilter;
use futures::{sink, Future};
use rocksdb::DB;
use std::{sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tx::{request_mempool_tx, tx_listener};

use crate::{
    error::MempoolErrors, filtertree::FilterTree, txtree::TxTree, worker::filters::filter_worker,
};

/// Main mempool worker
/// Receives Arcs to store its data in: `txtree`, `ftree`, `full_filter`
/// Receives handles to the UTXO set: cache + persistent db
/// `sync_mutex` is used to signal when the utxo is fully synced
/// `script_from_t` is used to convert Coin representation from `T` in the `UtxoCache` to `Script`
/// `filter_delay` and `hashmap_timeout` set the frequency of filter creation
/// and how long it is allowed to block utxo cache
/// Returns two computations for its two sub-worker: `tx_listener` and `filter_worker`
/// And also returns its message sink and stream for routing in the main function
pub async fn mempool_worker<T, M>(
    txtree: Arc<TxTree>,
    ftree: Arc<FilterTree>,
    full_filter: Arc<Mutex<Option<ErgveinMempoolFilter>>>,
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    sync_mutex: Arc<Mutex<()>>,
    script_from_t: M,
    filter_delay: Duration,
    hashmap_timeout: Duration,
) -> (
    impl Future<Output = Result<(), MempoolErrors>>,
    impl Future<Output = Result<(), MempoolErrors>>,
    impl futures::Sink<NetworkMessage, Error = encode::Error>,
    impl Unpin + futures::Stream<Item = NetworkMessage>,
)
where
    T: Decodable + Clone,
    M: Fn(&T) -> Script + Copy,
{
    const BUFFER_SIZE: usize = 100;
    let (broad_sender, _) = broadcast::channel(BUFFER_SIZE);
    let (msg_sender, msg_reciver) = mpsc::unbounded_channel::<NetworkMessage>();
    let tx_future = {
        let broad_sender = broad_sender.clone();
        let msg_sender = msg_sender.clone();
        let txtree = txtree.clone();
        async move {
            loop {
                tx_listener(txtree.clone(), &broad_sender, &msg_sender).await?;
            }
        }
    };
    let filt_future = {
        let broad_sender = broad_sender.clone();
        async move {
            filter_worker(
                txtree.clone(),
                &broad_sender,
                &msg_sender,
                ftree.clone(),
                full_filter.clone(),
                db.clone(),
                cache.clone(),
                sync_mutex.clone(),
                script_from_t,
                filter_delay,
                hashmap_timeout,
            )
            .await
        }
    };

    let msg_stream = UnboundedReceiverStream::new(msg_reciver);
    let msg_sink = sink::unfold(broad_sender, |broad_sender, msg| async move {
        broad_sender.send(msg).unwrap_or(0);
        Ok::<_, encode::Error>(broad_sender)
    });
    (tx_future, filt_future, msg_sink, msg_stream)
}

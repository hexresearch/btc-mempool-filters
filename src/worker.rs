use bitcoin::Transaction;
use bitcoin::Txid;
use bitcoin_utxo::cache::utxo::UtxoCache;
use bitcoin_utxo::cache::utxo::get_utxo_noh;
use bitcoin::consensus::Decodable;
use bitcoin::consensus::encode;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message::NetworkMessage;
use bitcoin::OutPoint;
use bitcoin::Script;
use bitcoin::util::bip158;
use ergvein_filters::mempool::ErgveinMempoolFilter;
use futures::Future;

use futures::future::AbortHandle;

use futures::future::Abortable;use futures::sink;
use rocksdb::DB;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

use crate::error::MempoolErrors;
use crate::filtertree::FilterTree;
use crate::filtertree::make_filters;
use crate::filtertree::make_full_filter;

use crate::txtree::get_transaction_script;
use crate::txtree::insert_tx;
use crate::txtree::TxTree;
use crate::txtree::remove_batch;
use crate::txtree::tx_tree_count;


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
    script_from_t : M,
    filter_delay: Duration,
    hashmap_timeout: Duration,
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
        let msg_sender = msg_sender.clone();
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
                hashmap_timeout
            ).await
        }
    };

    let msg_stream = UnboundedReceiverStream::new(msg_reciver);
    let msg_sink = sink::unfold(broad_sender, |broad_sender, msg| async move {
        broad_sender.send(msg).unwrap_or(0);
        Ok::<_, encode::Error>(broad_sender)
    });
    (tx_future, filt_future, msg_sink, msg_stream)
}

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
    script_from_t : M,
    filter_delay: Duration,
    hashmap_timeout: Duration,
) -> Result<(), MempoolErrors>
where
T:Decodable + Clone,
M:Fn(&T) -> Script + Copy
{
    println!("[filter_worker]: Starting");
    let mut hashmap = HashMap::<OutPoint, Script>::new();
    let mut timeout = hashmap_timeout;
    let mut err_cnt = 0;
    let mut succ_cnt = 0;
    loop {
        tokio::time::sleep(filter_delay).await;
        { // Make sure the utxo cache is fully synced before attempting to make filters
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
                    script_from_t)
                ,timeout_reg);
            tokio::select!{
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
            hashmap.get(out).map_or_else( || {
                Err(bip158::Error::UtxoMissing(*out))
            }, |s| Ok(s.clone()))

        });

        let ff = make_full_filter(&txtree, |out| {
            hashmap.get(out).map_or_else(|| Err(bip158::Error::UtxoMissing(*out)), |s| Ok(s.clone()))
        }).map_err(|e| eprintln!("Error making the full filter! {:?}", e)).ok();
        {
            let mut ffref = full_filter.lock().await;
            *ffref = ff;
            let l = ffref.as_ref().map(|f| f.content.len()).unwrap_or(0);
            if ffref.is_none() { err_cnt += 1; } else { succ_cnt +=1; }
            println!("Filter len: {:?}. Transactions in mempool: {}. Succ: {}. Error: {}", l, tx_tree_count(&txtree), succ_cnt, err_cnt);
        }
    };
}

/// Sub-worker. Listen to incoming messages for the node
/// Pick all new transactions from `Inv` messages and request them
/// Get transactions and add the to the `TxTree`
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
    };
}

/// Request a `Transaction` from the node's mempool
/// Re-request every 5 seconds
pub async fn request_mempool_tx(
    txid: Txid,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
) -> Result<Transaction, MempoolErrors>{
    let mut receiver = broad_sender.subscribe();
    println!("Requesting from node. Tx: {:?}", txid);
    msg_sender.send(NetworkMessage::GetData(vec![Inventory::Transaction(txid)])).map_err(
        |_| MempoolErrors::RequestTx
    )?;
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
    };
    res.ok_or(MempoolErrors::RequestTx)
}

/// Fill a hashmap with all input scripts for all txs in the txs tree (and more!)
/// Run fill_tx_input for all inputs
/// Add all extra transactions to extra stack and repeat the process untill the stack is empty
/// This is done because if there is a chain of transactions in the mempool and we request them during `fill_tx_input`
/// they get added to `TxTree` via `tx_listener` and we have to process their inputs also.
async fn fill_tx_map<T,M>(
    txtree: Arc<TxTree>,
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    hashmap: &mut HashMap<OutPoint, Script>,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
    script_from_t : M,
)
where
T:Decodable + Clone,
M:Fn(&T) -> Script + Copy
{
    let mut extra = Vec::new();
    for txs in txtree.iter(){
        for tx in txs.values(){
            if !tx.is_coin_base(){
                for i in tx.input.iter(){
                    let stx = fill_tx_input(
                        &i.previous_output,
                        txtree.clone(),
                        db.clone(),
                        cache.clone(),
                        hashmap,
                        broad_sender,
                        msg_sender,
                        script_from_t,
                    ).await;
                    stx.map(|tx| extra.push(tx));
                };
            }
        }
    }
    while !extra.is_empty() {
        let mut next_extra = Vec::new();
        for tx in extra.iter(){
            for i in tx.input.iter(){
                let stx = fill_tx_input(
                    &i.previous_output,
                    txtree.clone(),
                    db.clone(),
                    cache.clone(),
                    hashmap,
                    broad_sender,
                    msg_sender,
                    script_from_t,
                ).await;
                stx.map(|tx| next_extra.push(tx));
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
    script_from_t : M,
) -> Option<Transaction>
where
T:Decodable + Clone,
M:Fn(&T) -> Script + Copy
{
    let mut res = None;
    let coin = get_utxo_noh(
        &db.clone(),
        &cache.clone(),
        &i);
    match coin {
        Some(c) => {
            hashmap.insert(i.clone(), script_from_t(&c));
        }
        None => {
            let mscript = get_transaction_script(&txtree, i);
            match mscript{
                Some(script) => {hashmap.insert(i.clone(), script);},
                None => {
                    let stx = request_mempool_tx(i.txid, broad_sender, msg_sender).await;
                    match stx {
                        Ok(tx) => {
                            let script = tx.output[i.vout as usize].script_pubkey.clone();
                            hashmap.insert(i.clone(), script);
                            res = Some(tx);
                        },
                        Err(_) => {
                            eprintln!("Failed to get Tx {:?} from mempool", i.txid);
                        },
                    }
                }
            }
        }
    }
    res
}

/// Listens to incoming messages until it gets a `Block` message
/// Used to interrupt filter building
async fn wait_for_block(
    broad_sender: &broadcast::Sender<NetworkMessage>,
){
    let mut receiver = broad_sender.subscribe();
    loop {
        let emsg = receiver.recv().await;
        match emsg {
            Err(_) => {},
            Ok(msg) => match msg{
                NetworkMessage::Block(_) => break,
                _ => ()
            }

        }
    }
}

extern crate bitcoin;
extern crate bitcoin_utxo;
extern crate futures;

use bitcoin::consensus::encode::{self, Decodable, Encodable};
use bitcoin::network::constants;
use bitcoin::Script;
use bitcoin::{BlockHeader, Transaction};
use bitcoin_utxo::connection::connect;
use bitcoin_utxo::utxo::UtxoState;
use futures::future::{AbortHandle, Abortable};
use futures::pin_mut;
use mempool_filters::filtertree::*;
use mempool_filters::txtree::TxTree;
use mempool_filters::worker::mempool_worker;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct FilterCoin {
    pub script: Script,
}

impl UtxoState for FilterCoin {
    fn new_utxo(_height: u32, _header: &BlockHeader, tx: &Transaction, vout: u32) -> Self {
        FilterCoin {
            script: tx.output[vout as usize].script_pubkey.clone(),
        }
    }
}

impl Encodable for FilterCoin {
    fn consensus_encode<W: io::Write>(&self, writer: W) -> Result<usize, io::Error> {
        let len = self.script.consensus_encode(writer)?;
        Ok(len)
    }
}
impl Decodable for FilterCoin {
    fn consensus_decode<D: io::Read>(mut d: D) -> Result<Self, encode::Error> {
        Ok(FilterCoin {
            script: Decodable::consensus_decode(&mut d)?,
        })
    }
}

fn extract_script(fc: &FilterCoin) -> Script {
    fc.script.clone()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:8333".parse().unwrap();
    let dbname = "./db";
    println!("Opening database {:?}", dbname);
    let db = Arc::new(bitcoin_utxo::storage::init_storage(
        dbname.clone(),
        vec!["filters"],
    )?);
    println!("Creating cache");
    let cache = Arc::new(bitcoin_utxo::cache::utxo::new_cache::<FilterCoin>());

    let txtree = Arc::new(TxTree::new());
    let ftree = Arc::new(FilterTree::new());
    let full_filter = Arc::new(Mutex::new(None));
    let sync_mutex = Arc::new(Mutex::new(()));
    let filter_delay = Duration::from_secs(30);
    let hashmap_timeout = Duration::from_secs(10);
    let (tx_future, filt_future, msg_sink, msg_stream) = mempool_worker(
        txtree,
        ftree,
        full_filter,
        db,
        cache,
        sync_mutex,
        extract_script,
        filter_delay,
        hashmap_timeout,
    )
    .await;
    let (_, abort_server_reg) = AbortHandle::new_pair();

    tokio::spawn(async move {
        let res = tx_future.await;
        res.map_or_else(|e| eprintln!("ERROR: {:?}", e), |_| println!("DONE"))
    });

    tokio::spawn(async move {
        let res = filt_future.await;
        res.map_or_else(|e| eprintln!("ERROR: {:?}", e), |_| println!("DONE"))
    });

    pin_mut!(msg_sink);

    let res = Abortable::new(
        connect(
            &address,
            constants::Network::Bitcoin,
            "rust-client".to_string(),
            0,
            msg_stream,
            msg_sink,
        ),
        abort_server_reg,
    )
    .await;

    match res {
        Ok(_) => {
            println!("Done")
        }
        Err(e) => eprintln!("Nok {:?}", e),
    }
    Ok(())
}

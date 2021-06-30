extern crate bitcoin;
extern crate futures;
extern crate bitcoin_utxo;

use std::net::SocketAddr;
use std::str::FromStr;use std::sync::Arc;

use bitcoin::OutPoint;

use bitcoin::Script;
use bitcoin::network::constants;

use bitcoin_utxo::connection::connect;


use mempool_filters::filtertree::FilterTree;
use futures::pin_mut;

use mempool_filters::txtree::TxTree;

use futures::future::{AbortHandle, Abortable};

use tokio::time::Duration;

fn foo(o: &OutPoint) -> Result<Script, bitcoin::util::bip158::Error>{
    match Script::from_str(""){
        Ok(v) => Ok(v),
        Err(_) => Err(bitcoin::util::bip158::Error::UtxoMissing(*o))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let address: SocketAddr = "127.0.0.1:8333".parse().unwrap();

    let txtree = Arc::new(TxTree::new());
    let (sync_future, msg_sink, msg_stream) = mempool_filters::worker::sync_mempool(txtree.clone()).await;
    let (_, abort_server_reg) = AbortHandle::new_pair();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let res = sync_future.await;
        match res {
            Ok(_) => println!("DONE"),
            Err(e) => eprintln!("ERROR: {:?}", e)
        }
    });
    tokio::spawn(async move {
        let mut filt_tree = FilterTree::new();
        loop {
            let txtree = txtree.clone();
            tokio::time::sleep(Duration::from_secs(30)).await;
            {
                filt_tree.make_filters(&txtree, foo);
            }
            println!("{:?}", filt_tree.full_filter);
        }
    });
    pin_mut!(msg_sink);

    let res = Abortable::new(connect(
        &address,
        constants::Network::Bitcoin,
        "rust-client".to_string(),
        0,
        msg_stream,
        msg_sink,
    ),abort_server_reg).await;

    match res {
        Ok(_) => {println!("Done")},
        Err(e) => eprintln!("Nok {:?}", e)
    }
    Ok(())
}

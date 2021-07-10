use crate::filtertree::*;
use crate::txtree::*;
use bitcoin::{consensus::deserialize, hashes::hex::FromHex, OutPoint, Script, Transaction};
use chrono::Utc;
use ergvein_filters::util::is_script_indexable;
use std::io;
use std::io::BufRead;

#[test]
fn full_filter() {
    let txtree = TxTree::new();
    let txs = load_txs("./test/block1-txs");
    insert_tx_batch(&txtree, txs.clone(), Utc::now());
    let filter = make_full_filter(&txtree, emptyscripts).expect("Failed to match tx! ");
    let (k0, k1) = get_full_prefix();
    for tx in txs {
        let is_indexable = tx
            .output
            .iter()
            .any(|o| is_script_indexable(&o.script_pubkey));
        if is_indexable {
            let b = filter
                .match_tx_outputs(k0, k1, &tx)
                .expect("Filter matching error. ");
            assert!(b, "Failed to match tx! It didn't match when it should");
        }
    }
}

#[test]
fn bucket_filters() {
    let txtree = TxTree::new();
    let txs = load_txs("./test/block1-txs");
    insert_tx_batch(&txtree, txs.clone(), Utc::now());

    let ftree = FilterTree::new();
    make_filters(&ftree, &txtree, emptyscripts);

    for tx in txs.iter() {
        let is_indexable = tx
            .output
            .iter()
            .any(|o| is_script_indexable(&o.script_pubkey));
        if is_indexable {
            let pref = make_prefix(&tx.txid());
            let filter = ftree.get(&pref).expect("Failed to get a filter");
            let (k0, k1) = mk_siphash_keys(&pref);
            let b = filter
                .match_tx_outputs(k0, k1, &tx)
                .expect("Filter matching error. ");
            assert!(b, "Failed to match tx! It didn't match when it should");
        };
    }
}

fn load_txs(path: &str) -> Vec<Transaction> {
    let mut res = vec![];
    let file = std::fs::File::open(path).unwrap();
    for line in io::BufReader::new(file).lines() {
        let tx = deserialize(&Vec::from_hex(&line.unwrap()).unwrap()).unwrap();
        res.push(tx);
    }
    res
}

fn emptyscripts(_: &OutPoint) -> Result<Script, bitcoin::util::bip158::Error> {
    Ok(Script::new())
}

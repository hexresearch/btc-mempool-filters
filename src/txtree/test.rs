use crate::txtree::*;
use bitcoin::{consensus::deserialize, hashes::hex::FromHex, Transaction};
use std::io;
use std::io::BufRead;

#[test]
fn correct_inputs() {
    let mut txtree = TxTree::new();
    let txs = load_txs("./test/block1-txs");
    insert_tx_batch(&mut txtree, txs.clone());
    for tx in txs.iter() {
        let txid = &tx.txid();
        let pref = make_prefix(txid);
        let txs = txtree.get(&pref).expect("Failed to get tx bucket!");
        let tx2 = txs.get(txid).expect("Failed to get tx from bucket!");
        assert_eq!(tx, tx2);
    }
}

#[test]
fn correct_removal() {
    let mut txtree = TxTree::new();
    let txs = load_txs("./test/block1-txs");
    insert_tx_batch(&mut txtree, txs.clone());
    let (todel, tokeep) = txs.split_at(10);
    remove_batch(&txtree, &todel.to_vec());
    for tx in todel.iter() {
        let txid = &tx.txid();
        let pref = make_prefix(txid);
        match txtree.get(&pref) {
            // If it's the only tx with the prefix, the whole bucked is removed
            // So we have to check that
            None => assert_eq!(count_with_prefix(pref, &txs), 1),
            Some(bucket) => assert_eq!(None, bucket.get(txid)),
        }
    }
    for tx in tokeep.iter() {
        let txid = &tx.txid();
        let pref = make_prefix(txid);
        let txs = txtree.get(&pref).expect("Failed to get tx bucket!");
        let tx2 = txs.get(txid).expect("Failed to get tx from bucket!");
        assert_eq!(tx, tx2);
    }
}

fn count_with_prefix(pref: TxPrefix, txs: &Vec<Transaction>) -> usize {
    txs.iter()
        .filter(|tx| pref == make_prefix(&tx.txid()))
        .count()
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

use crate::txtree::*;
use bitcoin::{consensus::deserialize, hashes::hex::FromHex, Transaction};
use chrono::Utc;
use std::io;
use std::io::BufRead;

#[test]
fn correct_inputs() {
    let txtree = TxTree::new();
    let txs = load_txs("./test/block1-txs");
    insert_tx_batch(&txtree, txs.clone(), Utc::now());
    for tx in txs.iter() {
        let txid = &tx.txid();
        let pref = make_prefix(txid);
        let txs = txtree.get(&pref).expect("Failed to get tx bucket!");
        let (tx2, _) = txs.get(txid).expect("Failed to get tx from bucket!");
        assert_eq!(tx, tx2);
    }
}

#[test]
fn correct_removal() {
    let txtree = TxTree::new();
    let txs = load_txs("./test/block1-txs");
    insert_tx_batch(&txtree, txs.clone(), Utc::now());
    let (todel, tokeep) = txs.split_at(10);
    remove_batch(&txtree, &todel.to_vec());
    for tx in todel.iter() {
        let txid = &tx.txid();
        let pref = make_prefix(txid);
        match txtree.get(&pref) {
            // If there is no tx remaining in a branch, it is prunned.
            // So we have to check that
            None => assert_eq!(count_with_prefix(pref, &tokeep.to_vec()), 0),
            Some(bucket) => assert_eq!(None, bucket.get(txid)),
        }
    }
    for tx in tokeep.iter() {
        let txid = &tx.txid();
        let pref = make_prefix(txid);
        let txs = txtree.get(&pref).expect("Failed to get tx bucket!");
        let (tx2, _) = txs.get(txid).expect("Failed to get tx from bucket!");
        assert_eq!(tx, tx2);
    }
}

#[test]
fn stale_removal() {
    let txtree = TxTree::new();
    let txs = load_txs("./test/block1-txs");
    let (todel, tokeep) = txs.split_at(14);
    let t1 = Utc::now()
        .checked_sub_signed(Duration::hours(2))
        .expect("Failed to create timestamp");
    let t2 = Utc::now()
        .checked_sub_signed(Duration::weeks(3))
        .expect("Failed to create timestamp");
    insert_tx_batch(&txtree, tokeep.to_vec(), t1);
    insert_tx_batch(&txtree, todel.to_vec(), t2);
    remove_stale(&txtree);
    for tx in todel.iter() {
        let txid = &tx.txid();
        let pref = make_prefix(txid);
        match txtree.get(&pref) {
            // If there is no tx remaining in a branch, it is prunned.
            // So we have to check that
            None => assert_eq!(count_with_prefix(pref, &tokeep.to_vec()), 0),
            Some(bucket) => assert_eq!(None, bucket.get(txid)),
        }
    }
    for tx in tokeep.iter() {
        let txid = &tx.txid();
        let pref = make_prefix(txid);
        let txs = txtree.get(&pref).expect("Failed to get tx bucket!");
        let (tx2, _) = txs.get(txid).expect("Failed to get tx from bucket!");
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

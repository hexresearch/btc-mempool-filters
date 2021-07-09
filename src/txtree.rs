use std::collections::HashMap;
use bitcoin::{Script, OutPoint, Transaction, Txid};
use bitcoin_hashes::Hash;
use dashmap::DashMap;


pub const TX_PREFIX_BYTES : usize = 2;
pub const TX_PREFIX_MASK : [u8;TX_PREFIX_BYTES] = [0b11111111,0b11000000];

pub type TxPrefix = [u8;TX_PREFIX_BYTES];

pub type TxTree = DashMap<TxPrefix, HashMap<Txid, Transaction>>;

pub fn insert_tx(txtree: &TxTree, tx: &Transaction){
    let txid = tx.txid();
    let key = make_prefix(&txid);
    txtree.entry(key)
        .and_modify(|v| { v.insert(txid, tx.clone());})
        .or_insert_with(|| {
            let mut hm = HashMap::new();
            hm.insert(txid, tx.clone());
            hm
        });
}

pub fn tx_tree_count(txtree: &TxTree) -> usize{
    txtree.iter().map(|txs| txs.values().count()).sum()
}

pub fn insert_tx_batch (txtree: &TxTree, txs: Vec<Transaction>){
    txs.iter().for_each(|tx| insert_tx(txtree, tx));
}

pub fn remove_batch(txtree: &TxTree, txs: &Vec<Transaction>){
    txs.iter().for_each(|tx| {
        let txid = tx.txid();
        let key = make_prefix(&txid);
        txtree.entry(key).and_modify(|txs| {txs.remove(&txid);});
    });
    txtree.retain(|_,v| !v.is_empty());
}

pub fn make_prefix(txid: &Txid) -> TxPrefix{
    let mut key : TxPrefix = [0;TX_PREFIX_BYTES];
    key.copy_from_slice(&txid.into_inner()[0..TX_PREFIX_BYTES]);
    for i in 0..TX_PREFIX_BYTES {
        key[i] = key[i] &  TX_PREFIX_MASK[i]
    }
    return key;
}

pub fn get_transaction_script(txtree: &TxTree, out: &OutPoint) -> Option<Script>{
    let txid = &out.txid;
    let pref = make_prefix(txid);
    let txs = txtree.get(&pref)?;
    let tx = txs.get(txid)?;
    Some(tx.output[out.vout as usize].script_pubkey.clone())
}

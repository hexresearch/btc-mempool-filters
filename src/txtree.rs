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

pub fn tst () {
    let txid = [0b10101011, 0b10011101, 0b10011101, 0b10011101];
    let mut key : TxPrefix = [0;TX_PREFIX_BYTES];
    key.copy_from_slice(&txid[0..TX_PREFIX_BYTES]);
    for i in 0..TX_PREFIX_BYTES {
        key[i] = key[i] &  TX_PREFIX_MASK[i]
    }
    let lol : Vec<u8> = key.iter().cycle().take(8).cloned().collect();
    let k256 = bitcoin_hashes::sha256::Hash::hash(lol.as_slice()).into_inner();
    let (k128,_) = k256.split_at(16);
    let (k1,k2) = k128.split_at(8);
    println!("{:?}", k128);
    println!("{:?}{:?}", k1,k2);
    let q : Vec<String> = key.iter().map(|v| format!("{:b}", v)).collect();
    println!("{:?}", q);
}

pub fn get_transaction_script(txtree: &TxTree, out: &OutPoint) -> Option<Script>{
    let txid = &out.txid;
    let pref = make_prefix(txid);
    let txs = txtree.get(&pref)?;
    let tx = txs.get(txid)?;
    Some(tx.output[out.vout as usize].script_pubkey.clone())
}

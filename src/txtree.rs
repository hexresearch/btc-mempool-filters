use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Iter;
use bitcoin::{Transaction, Txid};
use bitcoin_hashes::Hash;

pub const TX_PREFIX_BYTES : usize = 2;
pub const TX_PREFIX_MASK : [u8;TX_PREFIX_BYTES] = [0b11111111,0b11000000];

pub type TxPrefix = [u8;TX_PREFIX_BYTES];

#[derive(Debug)]
pub struct TxTree {
    pub txtreemap : HashMap<TxPrefix, HashMap <Txid, Transaction>>
}

impl TxTree {
    pub fn new() -> TxTree{
        TxTree { txtreemap: HashMap::new() }
    }

    pub fn iter(&self) -> Iter<'_, TxPrefix, HashMap <Txid, Transaction> >{
        self.txtreemap.iter()
    }

    pub fn insert_batch (&mut self, txs: Vec<Transaction>){
        txs.iter().for_each(|tx| self.insert_single(tx));
    }

    pub fn insert_single(&mut self, tx : &Transaction){
        let txid = tx.txid();
        let mut key : TxPrefix = [0;TX_PREFIX_BYTES];
        key.copy_from_slice(&txid.into_inner()[0..TX_PREFIX_BYTES]);
        for i in 0..TX_PREFIX_BYTES {
            key[i] = key[i] &  TX_PREFIX_MASK[i]
        }
        self.txtreemap.entry(key)
            .and_modify(|v| { v.insert(txid, tx.clone());})
            .or_insert_with(|| {
                let mut hm = HashMap::new();
                hm.insert(txid, tx.clone());
                hm
            });
    }

    pub fn remove_single(&mut self, tx: &Transaction){
        let txid = tx.txid();
        let key = make_prefix(tx);
        self.txtreemap.entry(key).and_modify(|txs| {txs.remove(&txid);});
        let emp = self.txtreemap.get(&key).map(|txs| txs.is_empty()).unwrap_or(false);
        if emp {self.txtreemap.remove(&key);}
    }

    pub fn remove_batch (&mut self, txs: Vec<Transaction>){
        txs.iter().for_each(|tx| {
            let txid = tx.txid();
            let key = make_prefix(tx);
            self.txtreemap.entry(key).and_modify(|txs| {txs.remove(&txid);});
        });
        self.txtreemap.retain(|_,v| !v.is_empty());
    }
}

pub fn make_prefix(tx: &Transaction) -> TxPrefix{
    let txid = tx.txid();
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

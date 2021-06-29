use std::collections::HashMap;
use std::collections::hash_map::Iter;

use bitcoin::{Script, OutPoint};

use bitcoin_hashes::Hash;

use consensus_encode::util::endian;

use ergvein_filters::mempool::ErgveinMempoolFilter;

use crate::txtree::{TxPrefix, TxTree};

pub struct FilterTree {
    filt_tree : HashMap<TxPrefix, ErgveinMempoolFilter>,
    pub full_filter: Option<ErgveinMempoolFilter>
}

impl FilterTree {
    pub fn new() -> FilterTree{
        FilterTree{ filt_tree: HashMap::new(), full_filter: None}
    }

    fn get_full_prefix(&self) -> (u64, u64){
        let k0 = u64::from_le_bytes(*b"ergvein0");
        let k1 = u64::from_le_bytes(*b"filters0");
        (k0,k1)
    }

    pub fn make_filters<M>(&mut self, txtree: &TxTree, script_getter: M)
        where M: Fn(&OutPoint) -> Result<Script, bitcoin::util::bip158::Error>{
        self.filt_tree.clear();
        for (pref, txs) in txtree.iter(){
            let (k0, k1) = mk_siphash_keys(pref);
            let filt = ErgveinMempoolFilter::new_script_filter(k0, k1, txs.values().cloned().collect(), &script_getter);
            filt.map_or_else(|_| (), |f| {
                self.filt_tree.insert(pref.clone(), f);
            });
        }
        let (k0,k1) = self.get_full_prefix();
        let txs : Vec<bitcoin::Transaction> = txtree.tx_tree.values().map(|tmap| tmap.values()).flatten().cloned().collect();
        let filt = ErgveinMempoolFilter::new_script_filter(k0, k1, txs, &script_getter);
        self.full_filter = filt.ok();
    }

    pub fn iter(&self) -> Iter<'_, TxPrefix, ErgveinMempoolFilter >{
        self.filt_tree.iter()
    }
}

fn mk_siphash_keys(pref: &TxPrefix) -> (u64, u64){
    let seed : Vec<u8> = pref.iter().cycle().take(8).cloned().collect();
    let k256 = bitcoin_hashes::sha256::Hash::hash(seed.as_slice()).into_inner();
    let key_1 = endian::slice_to_u64_le(&k256[0..8]);
    let key_2 = endian::slice_to_u64_le(&k256[8..16]);
    (key_1,key_2)
}

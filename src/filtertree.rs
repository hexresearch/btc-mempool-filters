use std::collections::HashMap;

use bitcoin::{Txid, Transaction};

use bitcoin_hashes::Hash;

use consensus_encode::util::endian;

use ergvein_filters::btc::ErgveinFilter;

use crate::txtree::{TxPrefix, TxTree};

struct FilterTree {
    filt_tree : HashMap<TxPrefix, ErgveinFilter>
}

impl FilterTree {
    pub fn new() -> FilterTree{
        FilterTree{ filt_tree: HashMap::new()}
    }

    pub fn make_filters(&self, txs: &TxTree){

    }
}

fn make_filter(txs: &HashMap<Txid, Transaction>) -> ErgveinFilter{
    unimplemented!()
}


fn mk_siphash_keys(pref: &TxPrefix) -> (u64, u64){
    let seed : Vec<u8> = pref.iter().cycle().take(8).cloned().collect();
    let k256 = bitcoin_hashes::sha256::Hash::hash(seed.as_slice()).into_inner();
    let key_1 = endian::slice_to_u64_le(&k256[0..8]);
    let key_2 = endian::slice_to_u64_le(&k256[8..16]);
    (key_1,key_2)
}

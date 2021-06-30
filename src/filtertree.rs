use bitcoin::Transaction;
use bitcoin::{Script, OutPoint};

use bitcoin_hashes::Hash;

use consensus_encode::util::endian;

use dashmap::DashMap;
use ergvein_filters::mempool::ErgveinMempoolFilter;

use crate::txtree::{TxPrefix, TxTree};

pub type FilterTree = DashMap<TxPrefix, ErgveinMempoolFilter>;

pub fn get_full_prefix() -> (u64, u64){
    let k0 = u64::from_le_bytes(*b"ergvein0");
    let k1 = u64::from_le_bytes(*b"filters0");
    (k0,k1)
}

pub fn make_filters<M>(ftree: &FilterTree, txtree: &TxTree, script_getter: M)
    where M: Fn(&OutPoint) -> Result<Script, bitcoin::util::bip158::Error>{
        ftree.clear();
        txtree.into_iter().for_each(|kv| {
            let (pref,txs) = kv.pair();
            let (k0, k1) = mk_siphash_keys(pref);
            let filt = ErgveinMempoolFilter::new_script_filter(k0, k1, txs.values().cloned().collect(), &script_getter);
            filt.map_or_else(|_| (), |f| {
                ftree.insert(pref.clone(), f);
            });
        });
    }

pub fn make_full_filter<M>(txtree: &TxTree, script_getter: M) -> Result<ErgveinMempoolFilter, bitcoin::util::bip158::Error>
    where M: Fn(&OutPoint) -> Result<Script, bitcoin::util::bip158::Error>{
        let (k0,k1) = get_full_prefix();
        let txs : Vec<Transaction> = txtree.iter().flat_map(|tmap|
                tmap.values().cloned().collect::<Vec<Transaction>>()
            ).collect();
        ErgveinMempoolFilter::new_script_filter(k0, k1, txs, &script_getter)
    }

fn mk_siphash_keys(pref: &TxPrefix) -> (u64, u64){
    let seed : Vec<u8> = pref.iter().cycle().take(8).cloned().collect();
    let k256 = bitcoin_hashes::sha256::Hash::hash(seed.as_slice()).into_inner();
    let key_1 = endian::slice_to_u64_le(&k256[0..8]);
    let key_2 = endian::slice_to_u64_le(&k256[8..16]);
    (key_1,key_2)
}

use bitcoin::{OutPoint, Script, Transaction};
use bitcoin_hashes::Hash;
use consensus_encode::util::endian;
use dashmap::DashMap;
use ergvein_filters::mempool::ErgveinMempoolFilter;

use crate::txtree::{TxPrefix, TxTree};
#[cfg(test)]
mod test;

/// Main type. Stores filters to corresponding branches of `TxTree`
/// We store full filter separately to take advantage of DashMap's concurrency
pub type FilterTree = DashMap<TxPrefix, ErgveinMempoolFilter>;

/// Get keys for the full filter. This is basically a static function
pub fn get_full_prefix() -> (u64, u64) {
    let k0 = u64::from_le_bytes(*b"ergvein0");
    let k1 = u64::from_le_bytes(*b"filters0");
    (k0, k1)
}

/// Make filters for each branch of `TxTree` and store them in the corresponding branch of `FilterTree`
pub fn make_filters<M>(ftree: &FilterTree, txtree: &TxTree, script_getter: M)
where
    M: Fn(&OutPoint) -> Result<Script, bitcoin::util::bip158::Error>,
{
    ftree.clear();
    txtree.into_iter().for_each(|kv| {
        let (pref, txs) = kv.pair();
        let (k0, k1) = mk_siphash_keys(pref);
        let filt = ErgveinMempoolFilter::new_script_filter(
            k0,
            k1,
            txs.values().cloned().collect(),
            &script_getter,
        );
        filt.map_or_else(
            |_| (),
            |f| {
                ftree.insert(pref.clone(), f);
            },
        );
    });
}

/// Make full filter with all transactions in a `TxTree`.
/// This one does not require `FilterTree` but we still keep the function in this module
pub fn make_full_filter<M>(
    txtree: &TxTree,
    script_getter: M,
) -> Result<ErgveinMempoolFilter, bitcoin::util::bip158::Error>
where
    M: Fn(&OutPoint) -> Result<Script, bitcoin::util::bip158::Error>,
{
    let (k0, k1) = get_full_prefix();
    let txs: Vec<Transaction> = txtree
        .iter()
        .flat_map(|tmap| tmap.values().cloned().collect::<Vec<Transaction>>())
        .collect();
    ErgveinMempoolFilter::new_script_filter(k0, k1, txs, &script_getter)
}

/// Make siphash keys based on the prefix.
/// `TxPrefix` is 2 bytes, and we need 16 bytes, thus
/// First we cycle 2 prefix bytes to get 16 bytes seed
/// Then we take sha256(seed) and split it into two u64s.
pub fn mk_siphash_keys(pref: &TxPrefix) -> (u64, u64) {
    let seed: Vec<u8> = pref.iter().cycle().take(8).cloned().collect();
    let k256 = bitcoin_hashes::sha256::Hash::hash(seed.as_slice()).into_inner();
    let key_1 = endian::slice_to_u64_le(&k256[0..8]);
    let key_2 = endian::slice_to_u64_le(&k256[8..16]);
    (key_1, key_2)
}

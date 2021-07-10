use bitcoin::{OutPoint, Script, Transaction, Txid};
use bitcoin_hashes::Hash;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use std::collections::HashMap;

#[cfg(test)]
mod test;

/// Tx prefix length
pub const TX_PREFIX_BYTES: usize = 2;

/// Mask to apply to Tx prefix bytes. We only care about first 10 bits
pub const TX_PREFIX_MASK: [u8; TX_PREFIX_BYTES] = [0b11111111, 0b11000000];

/// Transactions prefix
pub type TxPrefix = [u8; TX_PREFIX_BYTES];

pub type TxTimestamp = DateTime<Utc>;

/// Main type. Represents the mempool with transactions grouped according to their prefix
pub type TxTree = DashMap<TxPrefix, HashMap<Txid, (Transaction, TxTimestamp)>>;

/// Insert a single transaction into `TxTree`
pub fn insert_tx(txtree: &TxTree, tx: &Transaction, timestamp: TxTimestamp) {
    let txid = tx.txid();
    let key = make_prefix(&txid);
    txtree
        .entry(key)
        .and_modify(|v| {
            v.insert(txid, (tx.clone(), timestamp));
        })
        .or_insert_with(|| {
            let mut hm = HashMap::new();
            hm.insert(txid, (tx.clone(), timestamp));
            hm
        });
}

/// Count total value of transactions in `TxTree`
pub fn tx_tree_count(txtree: &TxTree) -> usize {
    txtree.iter().map(|txs| txs.values().count()).sum()
}

/// Add a batch of transactions to `TxTree`
pub fn insert_tx_batch(txtree: &TxTree, txs: Vec<Transaction>, timestamp: TxTimestamp) {
    txs.iter().for_each(|tx| insert_tx(txtree, tx, timestamp));
}

/// Remove a batch of transactions from `TxTree`
/// Prune empty branches after `Transaction` removal
pub fn remove_batch(txtree: &TxTree, txs: &[Transaction]) {
    txs.iter().for_each(|tx| {
        let txid = tx.txid();
        let key = make_prefix(&txid);
        txtree.entry(key).and_modify(|txs| {
            txs.remove(&txid);
        });
    });
    txtree.retain(|_, v| !v.is_empty());
}

pub fn remove_stale(txtree: &TxTree) {
    let now = Utc::now();
    let stale_time = Duration::weeks(2);
    for mut txs in txtree.iter_mut() {
        txs.retain(|_, (_, ts)| now.signed_duration_since(*ts) < stale_time)
    }
    txtree.retain(|_, v| !v.is_empty());
}

/// Make `TxPrefix` from `&Txid`. Takes first `TX_PREFIX_BYTES` and applies `TX_PREFIX_MASK`
pub fn make_prefix(txid: &Txid) -> TxPrefix {
    let mut key: TxPrefix = [0; TX_PREFIX_BYTES];
    key.copy_from_slice(&txid.into_inner()[0..TX_PREFIX_BYTES]);
    for i in 0..TX_PREFIX_BYTES {
        key[i] &= TX_PREFIX_MASK[i]
    }
    key
}

/// Get the script associated with the `OutPoint`
pub fn get_transaction_script(txtree: &TxTree, out: &OutPoint) -> Option<Script> {
    let txid = &out.txid;
    let pref = make_prefix(txid);
    let txs = txtree.get(&pref)?;
    let (tx, _) = txs.get(txid)?;
    Some(tx.output[out.vout as usize].script_pubkey.clone())
}

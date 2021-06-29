use thiserror::Error;

#[derive(Error, Debug)]
pub enum MempoolErrors {
    #[error("Mempool request failed")]
    MempoolRequestFail,
}

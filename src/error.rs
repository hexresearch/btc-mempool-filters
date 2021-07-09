use thiserror::Error;

#[derive(Error, Debug)]
pub enum MempoolErrors {
    #[error("Failed to request Tx")]
    RequestTx,
}

use alloy_consensus::transaction::{Transaction as _, SignerRecoverable};
use serde::Serialize;
use reth_primitives::{Transaction as RethTransactionEnum, TransactionSigned, TxType};
use alloy_primitives::{Address, B256, U256};
use reth_tracing::tracing::warn;

#[derive(Debug, Clone, Serialize)]
pub struct TxAnalysisResult {
    pub hash: B256,
    pub tx_type: TxType,
    pub sender: Option<Address>,
    pub receiver: Option<Address>,
    pub value: U256,
    pub gas_limit: u64,
    pub gas_price_or_max_fee: Option<u128>,
    pub max_priority_fee: Option<u128>,
    pub input_len: usize,
    pub method_id: Option<String>,
    pub calldata_hex: Option<String>,
}

pub fn analyze_transaction_filtered(tx_signed: &TransactionSigned, include_input: bool) -> TxAnalysisResult {
    let hash = tx_signed.hash();

    let sender = match tx_signed.recover_signer() {
        Ok(addr) => Some(addr),
        Err(e) => {
            warn!(tx_hash=%hash, "Failed to recover sender: {}", e);
            None
        }
    };

    let receiver = tx_signed.to();
    let value = tx_signed.value();
    let gas_limit = tx_signed.gas_limit();
    let input_bytes = tx_signed.input();
    let input_len = input_bytes.len();
    let (method_id, calldata_hex) = if include_input {
        if input_len >= 4 {
            let mid = &input_bytes[..4];
            (
                Some(format!("0x{}", hex::encode(mid))),
                Some(format!("0x{}", hex::encode(input_bytes))),
            )
        } else if input_len > 0 {
            (None, Some(format!("0x{}", hex::encode(input_bytes))))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };
    let tx_type = tx_signed.tx_type();

    let unsigned_tx_enum = tx_signed.clone().into_typed_transaction();

    let (gas_price_or_max_fee, max_priority_fee) = match unsigned_tx_enum {
        RethTransactionEnum::Legacy(_) | RethTransactionEnum::Eip2930(_) => {
            (tx_signed.gas_price(), None)
        }
        RethTransactionEnum::Eip1559(_)
        | RethTransactionEnum::Eip4844(_)
        | RethTransactionEnum::Eip7702(_) => (
            Some(tx_signed.max_fee_per_gas()),
            tx_signed.max_priority_fee_per_gas(),
        ),
    };

    TxAnalysisResult {
        hash: *hash,
        tx_type,
        sender,
        receiver,
        value,
        gas_limit,
        gas_price_or_max_fee,
        max_priority_fee,
        input_len,
        method_id,
        calldata_hex,
    }
}

pub fn analyze_transaction(tx_signed: &TransactionSigned) -> TxAnalysisResult {
    analyze_transaction_filtered(tx_signed, true)
}



use anchor_lang::AnchorDeserialize;
use chrono::{DateTime, Utc};
use num_traits::Pow;
use tokio_postgres::Row;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct PgOpenBookFill {
    pub time: DateTime<Utc>,
    pub bid: bool,
    pub maker: bool,
    pub price: f64,
    pub size: f64,
}
impl PgOpenBookFill {
    pub fn from_row(row: Row) -> Self {
        PgOpenBookFill {
            time: row.get(0),
            bid: row.get(1),
            maker: row.get(2),
            price: row.get(3),
            size: row.get(4),
        }
    }
}

#[derive(Copy, Clone, AnchorDeserialize)]
#[cfg_attr(target_endian = "little", derive(Debug))]
#[repr(packed)]
pub struct MarketState {
    // 0
    pub account_flags: u64, // Initialized, Market

    // 1
    pub own_address: [u64; 4],

    // 5
    pub vault_signer_nonce: u64,
    // 6
    pub coin_mint: [u64; 4],
    // 10
    pub pc_mint: [u64; 4],

    // 14
    pub coin_vault: [u64; 4],
    // 18
    pub coin_deposits_total: u64,
    // 19
    pub coin_fees_accrued: u64,

    // 20
    pub pc_vault: [u64; 4],
    // 24
    pub pc_deposits_total: u64,
    // 25
    pub pc_fees_accrued: u64,

    // 26
    pub pc_dust_threshold: u64,

    // 27
    pub req_q: [u64; 4],
    // 31
    pub event_q: [u64; 4],

    // 35
    pub bids: [u64; 4],
    // 39
    pub asks: [u64; 4],

    // 43
    pub coin_lot_size: u64,
    // 44
    pub pc_lot_size: u64,

    // 45
    pub fee_rate_bps: u64,
    // 46
    pub referrer_rebates_accrued: u64,
}

pub fn token_factor(decimals: u8) -> f64 {
    10f64.pow(decimals as f64)
}

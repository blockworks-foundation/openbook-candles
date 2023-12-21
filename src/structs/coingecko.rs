use serde::Serialize;
use tokio_postgres::Row;

#[derive(Debug, Clone, Serialize)]
pub struct CoinGeckoOrderBook {
    pub ticker_id: String,
    pub timestamp: String, //as milliseconds
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoinGeckoPair {
    pub ticker_id: String,
    pub base: String,
    pub target: String,
    pub pool_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoinGeckoTicker {
    pub ticker_id: String,
    pub address: String,
    pub base_currency: String,
    pub target_currency: String,
    pub last_price: String,
    pub base_volume: String,
    pub target_volume: String,
    // pub bid: String,
    // pub ask: String,
    pub high: String,
    pub low: String,
}

#[derive(Debug, Default)]
pub struct PgCoinGecko24HourVolume {
    pub address: String,
    pub base_size: f64,
    pub quote_size: f64,
}
impl PgCoinGecko24HourVolume {
    pub fn from_row(row: Row) -> Self {
        PgCoinGecko24HourVolume {
            address: row.get(0),
            base_size: row.get(1),
            quote_size: row.get(2),
        }
    }
}

#[derive(Debug, Default)]
pub struct PgCoinGecko24HighLow {
    pub address: String,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

impl PgCoinGecko24HighLow {
    pub fn from_row(row: Row) -> Self {
        PgCoinGecko24HighLow {
            address: row.get(0),
            high: row.get(1),
            low: row.get(2),
            close: row.get(3),
        }
    }
}

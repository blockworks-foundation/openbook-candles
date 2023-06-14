use crate::structs::candle::Candle;

pub fn build_candles_upsert_statement(candles: &Vec<Candle>) -> String {
    let mut stmt = String::from("INSERT INTO openbook.candles (market_name, start_time, end_time, resolution, open, close, high, low, volume, complete) VALUES");
    for (idx, candle) in candles.iter().enumerate() {
        let val_str = format!(
            "(\'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {})",
            candle.market_name,
            candle.start_time.to_rfc3339(),
            candle.end_time.to_rfc3339(),
            candle.resolution,
            candle.open,
            candle.close,
            candle.high,
            candle.low,
            candle.volume,
            candle.complete,
        );

        if idx == 0 {
            stmt = format!("{} {}", &stmt, val_str);
        } else {
            stmt = format!("{}, {}", &stmt, val_str);
        }
    }

    let handle_conflict = "ON CONFLICT (market_name, start_time, resolution) 
    DO UPDATE SET 
    open=excluded.open, 
    close=excluded.close, 
    high=excluded.high, 
    low=excluded.low,
    volume=excluded.volume,
    complete=excluded.complete
    ";

    stmt = format!("{} {}", stmt, handle_conflict);
    stmt
}

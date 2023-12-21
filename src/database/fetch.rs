use crate::structs::{
    candle::Candle,
    coingecko::{PgCoinGecko24HighLow, PgCoinGecko24HourVolume},
    openbook::PgOpenBookFill,
    resolution::Resolution,
    trader::PgTrader,
};
use chrono::{DateTime, Utc};
use deadpool_postgres::{GenericClient, Pool};

pub async fn fetch_earliest_fill(
    pool: &Pool,
    market_address_string: &str,
) -> anyhow::Result<Option<PgOpenBookFill>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        block_datetime as "time",
        market as "market_key",
        bid as "bid",
        maker as "maker",
        price as "price",
        size as "size"
        from openbook.openbook_fill_events 
        where market = $1 
        and maker = true
        ORDER BY time asc LIMIT 1"#;

    let row = client.query_opt(stmt, &[&market_address_string]).await?;

    match row {
        Some(r) => Ok(Some(PgOpenBookFill::from_row(r))),
        None => Ok(None),
    }
}

pub async fn fetch_fills_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgOpenBookFill>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
         block_datetime as "time",
         market as "market_key",
         bid as "bid",
         maker as "maker",
         price as "price",
         size as "size"
         from openbook.openbook_fill_events 
         where market = $1
         and block_datetime >= $2::timestamptz
         and block_datetime < $3::timestamptz
         and maker = true
         ORDER BY time asc"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;
    Ok(rows.into_iter().map(PgOpenBookFill::from_row).collect())
}

pub async fn fetch_latest_finished_candle(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Option<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name",
        start_time as "start_time",
        end_time as "end_time",
        resolution as "resolution",
        open as "open",
        close as "close",
        high as "high",
        low as "low",
        volume as "volume",
        complete as "complete"
        from openbook.candles
        where market_name = $1
        and resolution = $2
        and complete = true
        ORDER BY start_time desc LIMIT 1"#;

    let row = client
        .query_opt(stmt, &[&market_name, &resolution.to_string()])
        .await?;

    match row {
        Some(r) => Ok(Some(Candle::from_row(r))),
        None => Ok(None),
    }
}

/// Fetches all of the candles for the given market and resolution, starting from the earliest.
/// Note that this function will fetch at most 2000 candles.
pub async fn fetch_earliest_candles(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
) -> anyhow::Result<Vec<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name",
        start_time as "start_time",
        end_time as "end_time",
        resolution as "resolution!",
        open as "open",
        close as "close",
        high as "high",
        low as "low",
        volume as "volume",
        complete as "complete"
        from openbook.candles
        where market_name = $1
        and resolution = $2
        ORDER BY start_time asc
        LIMIT 2000"#;

    let rows = client
        .query(stmt, &[&market_name, &resolution.to_string()])
        .await?;

    Ok(rows.into_iter().map(Candle::from_row).collect())
}

pub async fn fetch_candles_from(
    pool: &Pool,
    market_name: &str,
    resolution: Resolution,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
        market_name as "market_name",
        start_time as "start_time",
        end_time as "end_time",
        resolution as "resolution",
        open as "open",
        close as "close",
        high as "high",
        low as "low",
        volume as "volume",
        complete as "complete"
        from openbook.candles
        where market_name = $1
        and resolution = $2
        and start_time >= $3
        and end_time <= $4
        ORDER BY start_time asc"#;

    let rows = client
        .query(
            stmt,
            &[
                &market_name,
                &resolution.to_string(),
                &start_time,
                &end_time,
            ],
        )
        .await?;

    Ok(rows.into_iter().map(Candle::from_row).collect())
}

pub async fn fetch_top_traders_by_base_volume_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgTrader>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
            open_orders_owner, 
            sum(
            native_quantity_paid * CASE bid WHEN true THEN 0 WHEN false THEN 1 END
            ) as "raw_ask_size",
            sum(
            native_quantity_received * CASE bid WHEN true THEN 1 WHEN false THEN 0 END
            ) as "raw_bid_size"
        FROM openbook.openbook_fill_events
    WHERE  market = $1
            AND time >= $2
            AND time < $3
    GROUP  BY open_orders_owner
    ORDER  BY 
        sum(native_quantity_paid * CASE bid WHEN true THEN 0 WHEN false THEN 1 END) 
        + 
        sum(native_quantity_received * CASE bid WHEN true THEN 1 WHEN false THEN 0 END) 
    DESC 
    LIMIT 10000"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;

    Ok(rows.into_iter().map(PgTrader::from_row).collect())
}

pub async fn fetch_top_traders_by_quote_volume_from(
    pool: &Pool,
    market_address_string: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> anyhow::Result<Vec<PgTrader>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
            open_orders_owner, 
            sum(
                native_quantity_received * CASE bid WHEN true THEN 0 WHEN false THEN 1 END
            ) as "raw_ask_size",
            sum(
                native_quantity_paid * CASE bid WHEN true THEN 1 WHEN false THEN 0 END
            ) as "raw_bid_size"
          FROM openbook.openbook_fill_events
     WHERE  market = $1
            AND time >= $2
            AND time < $3
     GROUP  BY open_orders_owner
     ORDER  BY 
        sum(native_quantity_received * CASE bid WHEN true THEN 0 WHEN false THEN 1 END) 
        + 
        sum(native_quantity_paid * CASE bid WHEN true THEN 1 WHEN false THEN 0 END) 
    DESC  
    LIMIT 10000"#;

    let rows = client
        .query(stmt, &[&market_address_string, &start_time, &end_time])
        .await?;

    Ok(rows.into_iter().map(PgTrader::from_row).collect())
}

pub async fn fetch_coingecko_24h_volume(
    pool: &Pool,
    market_address_strings: &Vec<&str>,
) -> anyhow::Result<Vec<PgCoinGecko24HourVolume>> {
    let client = pool.get().await?;

    let stmt = r#"SELECT 
            t1.market, 
            COALESCE(t2.base_size, 0) as "base_size",
            COALESCE(t3.quote_size, 0) as "quote_size"
        FROM (
            SELECT unnest($1::text[]) as market 
        ) t1
        LEFT JOIN (
            select market,
            sum("size") as "base_size"
            from openbook.openbook_fill_events 
            where block_datetime >= current_timestamp - interval '1 day' 
            and bid = true
            group by market
        ) t2 ON t1.market = t2.market
        LEFT JOIN (
            select market,
            sum("size" * price) as "quote_size"
            from openbook.openbook_fill_events 
            where block_datetime >= current_timestamp - interval '1 day' 
            and bid = true
            group by market
        ) t3 ON t1.market = t3.market"#;

    let rows = client.query(stmt, &[&market_address_strings]).await?;

    Ok(rows
        .into_iter()
        .map(PgCoinGecko24HourVolume::from_row)
        .collect())
}

pub async fn fetch_coingecko_24h_high_low(
    pool: &Pool,
    market_address_strings: &Vec<&str>,
) -> anyhow::Result<Vec<PgCoinGecko24HighLow>> {
    let client = pool.get().await?;

    let stmt = r#"
    with markets as (
        select market, "close" from (
            select 
            ofe.market,
            price as "close", 
            instruction_num,
            row_number() over (partition by ofe.market order by ofe.instruction_num desc) as row_num
            from openbook.openbook_fill_events ofe 
            join (
                select market, max(block_datetime) as max_time
                from openbook.openbook_fill_events ofe 
                where market in (SELECT unnest($1::text[]))
                group by market
            ) x 
            on ofe.block_datetime = x.max_time
            and ofe.market = x.market
        ) x2 where row_num = 1
        ) 
        select 
            m.market as "address!", 
            coalesce(a.high, m."close") as "high!", 
            coalesce(a.low, m."close") as "low!", 
            coalesce(a."close", m."close") as "close!"
        from markets m
        left join 
        (
        select * from 
            (
                select 
                g.market,
                g.high,
                g.low,
                ofe.price as close,
                row_number() over (partition by ofe.market order by ofe.instruction_num desc) as row_num
                from openbook.openbook_fill_events ofe 
                join (
                    select 
                     market,
                     max(price) as "high",
                     min(price) as "low",
                     max(block_datetime) as max_time,
                     max(seq_num) as seq_num
                     from openbook.openbook_fill_events ofe 
                     where ofe.block_datetime > current_timestamp - interval '1 day'
                     and market in (SELECT unnest($1::text[]))
                     group by market
                ) g
                on g.market = ofe.market 
                and g.max_time = ofe.block_datetime
                and g.seq_num = ofe.seq_num
                join (
                    select 
                     market,
                     max(block_datetime) as max_time
                     from openbook.openbook_fill_events ofe 
                     where market in (SELECT unnest($1::text[]))
                     group by market
                ) b
                on b.market = ofe.market 
                and b.max_time = ofe.block_datetime
            ) a2
            where a2.row_num = 1
        ) a
        on a.market = m.market"#;

    let rows = client.query(stmt, &[&market_address_strings]).await?;

    Ok(rows
        .into_iter()
        .map(PgCoinGecko24HighLow::from_row)
        .collect())
}

use std::{fs, time::Duration};

use deadpool_postgres::{
    ManagerConfig, Pool, PoolConfig, RecyclingMethod, Runtime, SslMode, Timeouts,
};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;

use crate::utils::PgConfig;

pub async fn connect_to_database() -> anyhow::Result<Pool> {
    let mut pg_config = PgConfig::from_env()?;

    pg_config.pg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    pg_config.pg.pool = Some(PoolConfig {
        max_size: pg_config.pg_max_pool_connections,
        timeouts: Timeouts::default(),
    });

    // openssl pkcs12 -export -in client.cer -inkey client-key.cer -out client.pks
    // base64 -i ca.cer -o ca.cer.b64 && base64 -i client.pks -o client.pks.b64
    // fly secrets set PG_CA_CERT=- < ./ca.cer.b64 -a APP-NAME
    // fly secrets set PG_CLIENT_KEY=- < ./client.pks.b64 -a APP-NAME
    let tls = if pg_config.pg_use_ssl {
        pg_config.pg.ssl_mode = Some(SslMode::Require);
        let ca_cert = fs::read(pg_config.pg_ca_cert_path.expect("reading ca cert from env"))
            .expect("reading ca cert from file");
        let client_key = fs::read(
            pg_config
                .pg_client_key_path
                .expect("reading client key from env"),
        )
        .expect("reading client key from file");
        MakeTlsConnector::new(
            TlsConnector::builder()
                .add_root_certificate(Certificate::from_pem(&ca_cert)?)
                // TODO: make this configurable
                .identity(Identity::from_pkcs12(&client_key, "pass")?)
                .danger_accept_invalid_certs(false)
                .build()?,
        )
    } else {
        MakeTlsConnector::new(
            TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
        )
    };

    let pool = pg_config
        .pg
        .create_pool(Some(Runtime::Tokio1), tls)
        .unwrap();
    match pool.get().await {
        Ok(_) => println!("Database connected"),
        Err(e) => {
            println!("Failed to connect to database: {}, retrying", e.to_string());
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    Ok(pool)
}

pub async fn setup_database(pool: &Pool) -> anyhow::Result<()> {
    match create_candles_table(pool).await {
        Ok(_) => {
            println!("Successfully configured database");
            Ok(())
        }
        Err(e) => {
            println!("Failed to configure database: {e}");
            Err(e)
        }
    }
}

pub async fn create_candles_table(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS openbook.candles (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            market_name text,
            start_time timestamptz,
            end_time timestamptz,
            resolution text,
            open double precision,
            close double precision,
            high double precision,
            low double precision,
            volume double precision,
            complete bool
        )",
            &[],
        )
        .await?;

    client.execute(
        "CREATE UNIQUE INDEX idx_market_time_resolution ON openbook.candles USING btree (market_name, start_time, resolution);",
        &[]
    ).await?;

    Ok(())
}

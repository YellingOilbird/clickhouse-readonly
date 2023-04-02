use clickhouse_readonly::{ClickhouseResult, Pool, PoolConfig};

use futures_util::StreamExt;

#[tokio::main]
async fn main() -> ClickhouseResult<()> {
    std::env::set_var("RUST_LOG", "clickhouse_readonly=trace");
    env_logger::init();

    let pool = Pool::new(PoolConfig {
        addr: "127.0.0.1".parse().unwrap(),
        database: "default".to_string(),
        username: "username".to_string(),
        password: "password".to_string(),
        connection_timeout: std::time::Duration::from_secs(10u64),
        secure: true,
    });
    let mut handle = pool.get_handle().await?;

    let mut stream = handle.query("SELECT * FROM default.some_table").stream();

    while let Some(row) = stream.next().await {
        let row = row?;
        let asset: ethereum_types::Address = row.get("asset")?;
        let ticker: String = row.get("asset_symbol")?;
        let rate: ethereum_types::U256 = row.get("deposit")?;
        eprintln!("Found {ticker}: {asset:?} / rate: {rate:?}");
    }

    Ok(())
}

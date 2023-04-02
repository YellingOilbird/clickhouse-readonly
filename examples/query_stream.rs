use clickhouse_readonly::{ClickhouseResult, Pool, PoolConfigBuilder};

use futures_util::StreamExt;

#[tokio::main]
async fn main() -> ClickhouseResult<()> {
    std::env::set_var("RUST_LOG", "clickhouse_readonly=trace");
    env_logger::init();

    let config = PoolConfigBuilder::new(
        "127.0.0.1".parse().unwrap(),
        "default".to_string(),
        "username".to_string(),
        "password".to_string(),
        true,
    )
    .build();

    let pool = Pool::new(config);
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

## CLICKHOUSE-QUERY

Asynchronious TCP Connector to Clickhouse Database with `readonly` permissions (E.g. you can only execute queries but not made `Cmd::DATA` calls for `PacketStream`).

- `Date` or another Date Types are not supported.
- `Int256` provided by `ethnum::I256` and can be resolved only to `ethereum_types::U256`.
- `FixedString(42)` can be resolved to `ethereum_types::Address`

### Supported types:
```rust
pub enum SqlType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int256,
    String,
    FixedString,
    Float32,
    Float64,
    Nullable,
    Array,
}
```

#### <!> Crate is not planned to be update and can be used as it exists.

## Example

```sh
cargo run --package clickhouse-query --example query_stream 
```

```rust
use clickhouse_readonly::{ClickhouseResult, Pool, PoolConfig};

use futures_util::StreamExt;

#[tokio::main]
async fn main() -> ClickhouseResult<()> {
    std::env::set_var("RUST_LOG", "clickhouse_readonly=trace");
    env_logger::init();

    let config = PoolConfig::new(
        "127.0.0.1".parse().unwrap(),
        "default".to_string(),
        "username".to_string(),
        "password".to_string(),
        true,
    ).build();
    
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
```
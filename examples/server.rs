use std::time::Duration;

use anyhow::Result;
use redis::AsyncCommands;

#[tokio::main]
async fn main() -> Result<()> {
    let redis_url = "redis://redis:6379".to_string();
    let redis_client = redis::Client::open(redis_url)?;
    let mut con = redis_client.get_tokio_connection().await?;

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        con.publish("global".to_string(), "hello".to_string())
            .await?;
    }
}

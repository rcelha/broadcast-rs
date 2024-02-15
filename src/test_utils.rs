use std::time::Duration;

use super::server::{start_server, ServerConfig};
use anyhow::Result;
use futures::Future;
use tokio::{select, time::timeout};

/// Alias to create String from &str
pub use std::format as s;

/// Utility function  to write tests that rely on a running server.
///
/// ```ignore
/// #[tokio::test]
/// async fn test_pub_sub() -> Result<()> {
///    with_server(|| async {
///        Ok(())
///    }).await
/// }
/// ```
pub async fn with_server<F, G, O>(test_fn: F) -> Result<O>
where
    F: FnOnce(ServerConfig) -> G,
    G: Future<Output = Result<O>>,
{
    let config = server_config();
    let config_param = config.clone();
    let server_fut = start_server(config);

    let sleepy_test_fn = || async move {
        sleep_millis(10).await; // give the server some time to start
        test_fn(config_param).await
    };

    select! {
        _ = server_fut => { Err(anyhow::anyhow!("Server failed")) }
        test_result = sleepy_test_fn() => test_result,
    }
}

/// Utility function to test if a function times out (success case is when there is a timeout)
pub async fn with_timeout<F, G, O>(timeout_millis: u64, test_fn: F) -> Result<()>
where
    F: FnOnce() -> G,
    G: Future<Output = O>,
{
    if let Err(_) = timeout(Duration::from_millis(timeout_millis), test_fn()).await {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Test did not time out"))
    }
}

pub fn server_config() -> ServerConfig {
    ServerConfig {
        redis_url: s!("redis://localhost:6666/"),
        channel_capacity: 100,
        server_addr: s!("localhost"),
        server_port: 3000,
        statsd_host_port: None,
        otlp_endpoint: None,
    }
}

pub async fn sleep_millis(millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

pub trait ServerConfigTestExt {
    fn ws_url(&self, client_id: &str) -> String;
    fn redis_client(&self) -> Result<redis::Client>;
}

impl ServerConfigTestExt for ServerConfig {
    fn ws_url(&self, client_id: &str) -> String {
        format!(
            "ws://{}:{}/ws/{}",
            self.server_addr, self.server_port, client_id,
        )
    }

    fn redis_client(&self) -> Result<redis::Client> {
        Ok(redis::Client::open(self.redis_url.clone())?)
    }
}

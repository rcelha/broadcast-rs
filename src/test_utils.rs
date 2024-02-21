use std::time::Duration;

use crate::{
    broker_http::HttpBrokerConfig, broker_redis::RedisBrokerConfig, server::BackendConfig,
};

use super::server::{App, ServerConfig};
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
pub async fn with_server<F, G>(test_fn: F) -> Result<()>
where
    F: FnOnce(ServerConfig) -> G,
    G: Future<Output = Result<()>>,
{
    let config = server_config();
    let config_param = config.clone();
    let app = App::new(config);
    let server_fut = app.run();

    let sleepy_test_fn = || async move {
        sleep_millis(10).await; // give the server some time to start
        test_fn(config_param).await
    };

    select! {
        result = server_fut => result,
        test_result = sleepy_test_fn() => test_result,
    }
}

/// **TODO**
pub async fn with_server_http_backend<F, G>(test_fn: F) -> Result<()>
where
    F: FnOnce(ServerConfig) -> G,
    G: Future<Output = Result<()>>,
{
    let mut config = server_config();
    config.backend = BackendConfig::HttpBackend(HttpBrokerConfig {
        port: 9999,
        channel_capacity: 100,
    });
    let config_param = config.clone();
    let app = App::new(config);
    let server_fut = app.run();

    let sleepy_test_fn = || async move {
        sleep_millis(10).await; // give the server some time to start
        test_fn(config_param).await
    };

    select! {
        result = server_fut => result,
        test_result = sleepy_test_fn() => test_result,
    }
}

/// Utility function to test if a function times out (success case is when there is a timeout)
pub async fn with_timeout<F, G, O>(timeout_millis: u64, test_fn: F) -> Result<()>
where
    F: FnOnce() -> G,
    G: Future<Output = O>,
{
    if timeout(Duration::from_millis(timeout_millis), test_fn())
        .await
        .is_err()
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Test did not time out"))
    }
}

pub fn server_config() -> ServerConfig {
    ServerConfig {
        backend: BackendConfig::RedisBackend(RedisBrokerConfig {
            redis_url: "redis://localhost:6666/".to_string(),
            channel_capacity: 100,
        }),
        server_addr: "localhost".to_string(),
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
    fn http_admin_url(&self) -> Result<String>;
}

impl ServerConfigTestExt for ServerConfig {
    fn ws_url(&self, client_id: &str) -> String {
        format!(
            "ws://{}:{}/ws/{}",
            self.server_addr, self.server_port, client_id,
        )
    }

    fn redis_client(&self) -> Result<redis::Client> {
        let redis_url = match &self.backend {
            BackendConfig::RedisBackend(config) => Ok(config.redis_url.clone()),
            _ => Err(anyhow::anyhow!("Not a RedisBackend")),
        }?;
        Ok(redis::Client::open(redis_url)?)
    }

    fn http_admin_url(&self) -> Result<String> {
        let port = match &self.backend {
            BackendConfig::HttpBackend(config) => Ok(config.port),
            _ => Err(anyhow::anyhow!("Not a RedisBackend")),
        }?;
        Ok(format!("http://localhost:{}/", port))
    }
}

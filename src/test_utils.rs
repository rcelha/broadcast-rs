use std::time::Duration;

use crate::start_server;
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
///    }.await
/// }
/// ```
pub async fn with_server<F, G, O>(test_fn: F) -> Result<O>
where
    F: FnOnce() -> G,
    G: Future<Output = Result<O>>,
{
    select! {
        _ = start_server() => { Err(anyhow::anyhow!("Server failed")) }
        test_result = test_fn() => test_result,
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

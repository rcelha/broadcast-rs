use anyhow::Result;
use broadcast_rs::test_utils::*;
use futures::{SinkExt, StreamExt};

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::test]
async fn test_echo_is_disabled() -> Result<()> {
    with_server(|server_config| async move {
        let url = server_config.ws_url("client-id-1234");
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        write.send(Message::Text(s!("Hello"))).await?;
        let _ = with_timeout(100, || async move { read.next().await }).await?;
        Ok(())
    })
    .await
}

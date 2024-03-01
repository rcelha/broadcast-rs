use anyhow::Result;
use broadcast_rs::test_utils::*;
use futures::{SinkExt, StreamExt};
use redis::AsyncCommands;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::test]
async fn test_crash_on_disconected_client_channel() -> Result<()> {
    with_server(|server_config| async move {
        let url = server_config.ws_url("client-id-1234");
        let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        ws_stream
            .send(Message::Text(s!("subscribe:client-id-1234")))
            .await?;
        ws_stream.close(None).await?;

        {
            let client = server_config.redis_client()?;
            let mut con = client.get_tokio_connection().await?;

            sleep_millis(100).await;
            con.publish(s!("client-id-1234"), s!("hello")).await?;
        };

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_client_reconnect_gets_new_messages() -> Result<()> {
    with_server(|server_config| async move {
        let url = server_config.ws_url("client-id-1234");

        let (mut ws_stream, _) = connect_async(&url).await.expect("Failed to connect");
        ws_stream
            .send(Message::Text(s!("subscribe:global")))
            .await?;
        ws_stream.close(None).await?;

        let (ws_stream, _) = connect_async(&url).await.expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();
        write.send(Message::Text(s!("subscribe:global"))).await?;

        let url = server_config.ws_url("client-id-5678");
        let (ws_stream2, _) = connect_async(url).await.expect("Failed to connect");
        let (mut write2, mut read2) = ws_stream2.split();
        write2.send(Message::Text(s!("subscribe:global"))).await?;

        let client = server_config.redis_client()?;
        let mut con = client.get_tokio_connection().await?;

        let pub_fut = tokio::spawn(async move {
            sleep_millis(100).await;
            con.publish(s!("global"), s!("hello")).await?;
            Ok::<(), anyhow::Error>(())
        });
        let message = read.next().await.unwrap()?.into_text()?;
        let message2 = read2.next().await.unwrap()?.into_text()?;

        pub_fut.await??;

        assert_eq!(message, "hello");
        assert_eq!(message2, "hello");
        Ok(())
    })
    .await
}

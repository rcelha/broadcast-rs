use anyhow::Result;
use broadcast_rs::test_utils::*;
use futures::{SinkExt, StreamExt};
use redis::AsyncCommands;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::test]
async fn test_pub_sub() -> Result<()> {
    with_server(|server_config| async move {
        let url = server_config.ws_url("client-id-1234");
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();

        write.send(Message::Text(s!("subscribe:global"))).await?;
        write.send(Message::Text(s!("subscribe:global"))).await?;

        let client = server_config.redis_client()?;
        let mut con = client.get_tokio_connection().await?;

        let pub_fut = tokio::spawn(async move {
            sleep_millis(100).await;
            con.publish(s!("global"), s!("hello")).await?;
            con.publish(s!("global"), s!("hello 2")).await?;
            Ok::<(), anyhow::Error>(())
        });
        let message = read.next().await.unwrap()?.into_text()?;
        let message2 = read.next().await.unwrap()?.into_text()?;

        pub_fut.await??;

        assert_eq!(message, "hello");
        assert_eq!(message2, "hello 2");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn http_pub_sub() -> Result<()> {
    with_server_http_backend(|server_config| async move {
        let url = server_config.ws_url("client-id-1234");
        let backend_url = server_config.http_admin_url()?;
        let req_client = reqwest::Client::builder().build()?;

        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();

        write.send(Message::Text(s!("subscribe:global"))).await?;
        write.send(Message::Text(s!("subscribe:global"))).await?;

        let pub_fut = tokio::spawn(async move {
            sleep_millis(100).await;
            let publish = |body: String| async {
                let resp = req_client
                    .post(format!("{}channels/global/messages", backend_url))
                    .body(body)
                    .send()
                    .await?;
                assert!(resp.status().is_success());
                anyhow::Ok(())
            };
            publish(s!("hello")).await?;
            publish(s!("hello 2")).await?;
            Ok::<(), anyhow::Error>(())
        });
        let message = read.next().await.unwrap()?.into_text()?;
        let message2 = read.next().await.unwrap()?.into_text()?;

        pub_fut.await??;

        assert_eq!(message, "hello");
        assert_eq!(message2, "hello 2");
        Ok(())
    })
    .await
}

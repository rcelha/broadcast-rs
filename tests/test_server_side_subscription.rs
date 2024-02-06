use anyhow::Result;
use broadcast_rs::test_utils::{s, with_server, with_timeout};
use futures::StreamExt;
use redis::AsyncCommands;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;

#[tokio::test]
async fn test_subscribe_admin_command() -> Result<()> {
    with_server(|| async {
        let url = "ws://localhost:3000/ws/client-id-1234";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (_, mut read) = ws_stream.split();

        let client = redis::Client::open("redis://localhost:6666/")?;
        let mut con = client.get_tokio_connection().await?;

        let pub_fut = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            con.publish(s!("admin"), s!("subscribe:client-id-1234:global"))
                .await?;
            sleep(Duration::from_millis(100)).await;
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
async fn test_unsubscribe_admin_command() -> Result<()> {
    with_server(|| async {
        let url = "ws://localhost:3000/ws/client-id-2";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (_, mut read) = ws_stream.split();

        let client = redis::Client::open("redis://localhost:6666/")?;
        let mut con = client.get_tokio_connection().await?;

        let pub_fut = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            con.publish(s!("admin"), s!("subscribe:client-id-2:global"))
                .await?;

            sleep(Duration::from_millis(100)).await;
            con.publish(s!("global"), s!("hello")).await?;

            sleep(Duration::from_millis(100)).await;
            con.publish(s!("admin"), s!("unsubscribe:client-id-2:global"))
                .await?;

            // As the client is unsubscribed, this message should not be received
            sleep(std::time::Duration::from_millis(100)).await;
            con.publish(s!("global"), s!("hello 2")).await?;
            Ok::<(), anyhow::Error>(())
        });

        let message = read.next().await.unwrap()?.into_text()?;
        assert_eq!(message, "hello");
        let _ = with_timeout(500, || async move { read.next().await }).await?;

        pub_fut.await??;
        Ok(())
    })
    .await
}

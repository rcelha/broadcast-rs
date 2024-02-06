use anyhow::Result;
use broadcast_rs::test_utils::{s, with_server};
use futures::{SinkExt, StreamExt};
use redis::AsyncCommands;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::test]
async fn test_pub_sub() -> Result<()> {
    with_server(|| async {
        let url = "ws://localhost:3000/ws/client-id-1234";
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();

        write.send(Message::Text(s!("subscribe:global"))).await?;
        write.send(Message::Text(s!("subscribe:global"))).await?;

        let client = redis::Client::open("redis://localhost:6666/")?;
        let mut con = client.get_tokio_connection().await?;

        let pub_fut = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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

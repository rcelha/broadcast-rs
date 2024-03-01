use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "ws://broadcast:3000/ws/client-1";
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    write
        .send(Message::Text("subscribe:global".to_string()))
        .await?;
    write
        .send(Message::Text("subscribe:private-client-1".to_string()))
        .await?;

    let mut msg_count = 5;
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(msg)) => {
                println!("Received: {:?}", msg);
                // every 5 messages, send a ping
                msg_count -= 1;
                if msg_count == 0 {
                    msg_count = 5;
                    write.send(Message::Ping(vec![])).await?;
                }
            }
            Ok(Message::Close(_)) => {
                break;
            }
            _ => {
                // do nothing
            }
        }
    }
    Ok(())
}

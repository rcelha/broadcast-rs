use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use broker::BrokerApi;

use std::net::SocketAddr;

//allows to extract the IP of connecting user
use axum::extract::connect_info::ConnectInfo;
use tokio::select;

//allows to split the websocket stream into separate TX and RX branches
use futures::{sink::SinkExt, stream::StreamExt};

pub mod broker;

#[cfg(feature = "test")]
pub mod test_utils;

#[derive(Clone)]
struct AppState {
    broker: BrokerApi,
}

pub async fn start_server() -> Result<()> {
    let mut broker_config = broker::RedisBroker::config();
    broker_config.redis_url = "redis://localhost:6666/".to_string();
    broker_config.channel_capacity = 100;
    let mut broker = broker_config.build()?;

    let app_state = AppState {
        broker: broker.api(),
    };
    let app = Router::new()
        .route("/ws/:user_id", get(ws_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    let http_server = || async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    };

    select! {
        result = http_server() => { result.expect("Server failed")},
        result = broker.run() => { result.expect("Broker failed")}
    };

    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    connect_info: ConnectInfo<SocketAddr>,
    Path(user_id): Path<String>,
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    println!("new client connected: {}", connect_info.0.to_string());
    ws.on_upgrade(|socket| async move {
        match serve_client(socket, user_id, app_state).await {
            Ok(_) => println!("Client disconnected"),
            Err(e) => eprintln!("Client Error: {}", e),
        }
    })
}

async fn serve_client(socket: WebSocket, user_id: String, app_state: AppState) -> Result<()> {
    let (mut socket_tx, mut socket_rx) = socket.split();
    let (inner_tx, mut inner_rx) = tokio::sync::mpsc::channel::<Message>(100);

    // This channel makes receiving and sending async
    tokio::spawn(async move {
        // pool from broker to client
        while let Some(msg) = inner_rx.recv().await {
            socket_tx.send(msg).await.unwrap();
        }
    });

    // Connect WS to broker
    let client_tx = app_state.broker.connect_client(user_id.clone()).await?;
    let mut client_rx = client_tx.subscribe();
    let bridge_tx = inner_tx.clone();
    tokio::spawn(async move {
        while let Ok(msg) = client_rx.recv().await {
            bridge_tx.send(Message::Text(msg)).await.unwrap();
        }
    });

    while let Some(Ok(msg)) = socket_rx.next().await {
        match msg {
            Message::Text(text) => {
                let mut cmd_iter = text.trim().split(':');
                let cmd_name = cmd_iter.next().ok_or(anyhow::anyhow!("No command"))?;
                match cmd_name {
                    "subscribe" => {
                        let channel = cmd_iter
                            .next()
                            .ok_or(anyhow::anyhow!("No channel"))?
                            .to_string();
                        app_state.broker.subscribe(user_id.clone(), channel).await?;
                    }
                    "unsubscribe" => {
                        let channel = cmd_iter
                            .next()
                            .ok_or(anyhow::anyhow!("No channel"))?
                            .to_string();
                        app_state
                            .broker
                            .unsubscribe(user_id.clone(), channel)
                            .await?;
                    }
                    _ => {}
                }
            }
            Message::Binary(bin) => {
                inner_tx.send(Message::Binary(bin)).await?;
            }
            // TODO:: used as keep alive
            Message::Ping(ping) => {
                inner_tx.send(Message::Pong(ping)).await?;
            }
            Message::Pong(_) => {
                // do nothing
            }
            Message::Close(close) => {
                app_state.broker.disconnect_client(user_id.clone()).await?;
                inner_tx.send(Message::Close(close)).await?;
            }
        }
    }
    Ok(())
}

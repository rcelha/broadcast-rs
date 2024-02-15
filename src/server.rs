use super::broker::{BrokerApi, RedisBroker};
use anyhow::Result;
use axum::extract::connect_info::ConnectInfo;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{sink::SinkExt, stream::StreamExt};
use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};
use tokio::select;

// statsd
use cadence::{prelude::*, BufferedUdpMetricSink, NopMetricSink, QueuingMetricSink, StatsdClient};

// tracing
use opentelemetry::{global::shutdown_tracer_provider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Clone)]
struct AppState {
    broker: BrokerApi,
    statsd: Arc<StatsdClient>,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub redis_url: String,
    pub channel_capacity: usize,
    pub server_addr: String,
    pub server_port: u16,
    pub statsd_host_port: Option<(String, u16)>,
    pub otlp_endpoint: Option<String>,
}

fn setup_statsd_client(config: &ServerConfig) -> Result<StatsdClient> {
    if let Some(host) = &config.statsd_host_port {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
        socket.set_nonblocking(true)?;
        let udp_sink = BufferedUdpMetricSink::from(host, socket)?;
        let sink = QueuingMetricSink::with_capacity(udp_sink, 1);
        let statsd = StatsdClient::from_sink("statsd", sink);
        Ok(statsd)
    } else {
        let sink = NopMetricSink {};
        let statsd = StatsdClient::from_sink("statsd", sink);
        Ok(statsd)
    }
}

fn setup_tracer(config: &ServerConfig) -> Result<()> {
    let stdout_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(EnvFilter::from_default_env());

    let registry = tracing_subscriber::registry().with(stdout_layer);

    if let Some(endpoint) = &config.otlp_endpoint {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint.clone()),
            )
            .with_trace_config(
                opentelemetry_sdk::trace::config().with_resource(Resource::new(vec![
                    KeyValue::new("service.name", "broadcast"),
                ])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;
        let otlp_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let registry = registry.with(otlp_layer);
        registry.init();
    } else {
        registry.init();
    }
    Ok(())
}

fn setup_broker(config: &ServerConfig) -> Result<RedisBroker> {
    let mut broker_config = RedisBroker::config();
    broker_config.redis_url = config.redis_url.clone();
    broker_config.channel_capacity = config.channel_capacity;
    let broker = broker_config.build()?;
    Ok(broker)
}

/// TODO
pub async fn start_server(config: ServerConfig) -> Result<()> {
    let statsd = setup_statsd_client(&config)?;
    let mut broker = setup_broker(&config)?;
    setup_tracer(&config)?;

    let app_state = AppState {
        broker: broker.api(),
        statsd: Arc::new(statsd),
    };

    let app = Router::new()
        .route("/ws/:user_id", get(ws_handler))
        .layer(
            TraceLayer::new_for_http().make_span_with(DefaultMakeSpan::new().include_headers(true)),
        )
        .with_state(app_state);

    let addr = format!("{}:{}", config.server_addr, config.server_port);
    tracing::debug!("Starting server on: {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
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

    shutdown_tracer_provider();
    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    _connect_info: ConnectInfo<SocketAddr>,
    Path(user_id): Path<String>,
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| async move {
        match serve_client(socket, user_id, app_state).await {
            Ok(_) => tracing::info!("Client disconnected"),
            Err(e) => tracing::error!("Client Error: {}", e),
        }
    })
}

/// This function is responsible for handling the websocket connection
///
/// It will run a few tasks on background
///
/// The tasks are:
/// - A task to forward messages from the broker to the client
/// - A task to forward messages from the client to the broker
/// - A loop to receive and handle messages from the client
async fn serve_client(socket: WebSocket, user_id: String, app_state: AppState) -> Result<()> {
    let statsd = app_state.statsd.clone();

    let (mut socket_tx, mut socket_rx) = socket.split();
    let (bridge_tx, mut bridge_rx) = tokio::sync::mpsc::channel::<Message>(100);

    // This tasks forwards messages using an internal mpsc channel to the client
    // This way, we can receive and send messages to the client in an async way
    let inner_statsd = statsd.clone();
    let msg_routing_task = async move {
        while let Some(msg) = bridge_rx.recv().await {
            if let Err(e) = socket_tx.send(msg).await {
                tracing::error!("Error sending message to client's socket: {}", e);
                tracing::error!("Assuming disconnection");
                break;
            }
            tracing::debug!("Message sent to client");
            inner_statsd.count("messages_sent", 1).ok();
        }

        // The brigde_rx channel is closed, so we assume the client is disconnected
        // Let's send him a message so he knows he's been disconnected
        socket_tx.send(Message::Close(None)).await.ok();
        anyhow::Ok(())
    };

    // Brige client to broker
    // It receives messages from the broker and sends to the client mpsc channel
    // This message will eventually be picked up by the task above and sent to the
    // client
    let client_broker_tx = app_state.broker.connect_client(user_id.clone()).await?;
    let inner_bridge_tx = bridge_tx.clone();
    let bridge_task = async move {
        let mut client_broker_rx = client_broker_tx.subscribe();
        while let Ok(msg) = client_broker_rx.recv().await {
            if let Err(e) = inner_bridge_tx.send(Message::Text(msg)).await {
                tracing::error!("Error sending message to client's mpsc channel: {}", e);
                tracing::error!("Assuming disconnection");
                break;
            }
            tracing::debug!("Message sent to internal client channel");
        }
        // The client_broker_rx channel is closed, so we assume the client is disconnected
        inner_bridge_tx.send(Message::Close(None)).await.ok();
        anyhow::Ok(())
    };

    // This task is responsible for constantly receive PING messages from the client
    // and control wethever the client is still connected or not
    // Every 1 second, it will verify if there has been any activity from the client
    // It will finish if there's no activity for more than 10 seconds
    let last_seen = Arc::new(RwLock::new(std::time::Instant::now()));
    let inner_last_seen = last_seen.clone();
    let ping_pong_task = async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            if inner_last_seen
                .read()
                .expect("lock poisoned")
                .elapsed()
                .as_secs()
                > 10
            {
                tracing::error!("No activities from the client for more than 10 seconds");
                tracing::error!("Assuming disconnection");
                break;
            }
        }
        anyhow::Ok(())
    };

    let update_last_seen = move || {
        *last_seen.write().expect("lock poisoned") = std::time::Instant::now();
    };
    let broker_api = app_state.broker.clone();
    let inner_user_id = user_id.clone();
    let serve_loop = async move {
        while let Some(Ok(msg)) = socket_rx.next().await {
            update_last_seen();
            statsd.count("messages_received", 1).ok();
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
                            broker_api.subscribe(inner_user_id.clone(), channel).await?;
                        }
                        "unsubscribe" => {
                            let channel = cmd_iter
                                .next()
                                .ok_or(anyhow::anyhow!("No channel"))?
                                .to_string();
                            broker_api
                                .unsubscribe(inner_user_id.clone(), channel)
                                .await?;
                        }
                        _ => {}
                    }
                }
                Message::Binary(bin) => {
                    bridge_tx.send(Message::Binary(bin)).await?;
                }
                Message::Ping(ping) => {
                    bridge_tx.send(Message::Pong(ping)).await?;
                }
                Message::Pong(_) => {}
                Message::Close(close) => {
                    broker_api.disconnect_client(inner_user_id.clone()).await?;
                    bridge_tx.send(Message::Close(close)).await?;
                }
            }
        }
        anyhow::Ok(())
    };

    select! {
        result = bridge_task => result,
        result = serve_loop => result,
        result = msg_routing_task  => result,
        result = ping_pong_task => result,
    }?;

    app_state.broker.disconnect_client(user_id).await?;
    Ok(())
}

use anyhow::Result;
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    routing::{delete, post},
    Router,
};
use futures::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{broker::Broker, broker_api::BrokerCommand, client_membership::ClientMembership};

type ClientMessage = Arc<String>;

#[derive(Clone, Default, Debug)]
pub struct HttpBrokerConfig {
    pub port: u16,
    pub channel_capacity: usize,
}

impl HttpBrokerConfig {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct HttpBroker {
    pub port: u16,
    pub channel_capacity: usize,
    pub members: ClientMembership,

    /// (channel, client_id)
    pub api_tx: mpsc::UnboundedSender<BrokerCommand>,
    pub api_rx: mpsc::UnboundedReceiver<BrokerCommand>,
}

impl TryFrom<HttpBrokerConfig> for HttpBroker {
    type Error = anyhow::Error;

    fn try_from(val: HttpBrokerConfig) -> std::prelude::v1::Result<Self, Self::Error> {
        let (api_tx, api_rx) = mpsc::unbounded_channel();

        Ok(HttpBroker {
            port: val.port,
            channel_capacity: val.channel_capacity,
            api_tx,
            api_rx,
            members: ClientMembership::new(),
        })
    }
}

impl HttpBroker {}

impl Broker for HttpBroker {
    type Config = HttpBrokerConfig;

    fn api_tx(&self) -> &mpsc::UnboundedSender<BrokerCommand> {
        &self.api_tx
    }

    fn api_rx(&mut self) -> &mut mpsc::UnboundedReceiver<BrokerCommand> {
        &mut self.api_rx
    }

    async fn handle_connect_client(
        &self,
        client_id: String,
        tx: oneshot::Sender<broadcast::Sender<ClientMessage>>,
    ) -> Result<()> {
        self.members
            .add_client(client_id.clone(), tx, self.channel_capacity)
    }

    async fn handle_disconnect_client(&self, client_id: String) -> Result<()> {
        self.members.remove_client(client_id);
        Ok(())
    }

    async fn handle_subscribe(&self, client_id: String, channel: String) -> Result<()> {
        self.members
            .add_client_subscription(client_id.clone(), channel.clone());
        Ok(())
    }

    async fn handle_unsubscribe(&self, client_id: String, channel: String) -> Result<()> {
        self.members.remove_client_subscription(client_id, channel);
        Ok(())
    }

    fn run_backend_server<'broker, 'fut: 'broker>(
        &'broker self,
    ) -> impl Future<Output = Result<()>> + Send + 'fut {
        let port = self.port;
        let members = self.members.clone();
        let app_state = BrokerHttpState { members };

        async move {
            let router = Router::new()
                .route(
                    "/admin/subscription/:user_id/:channel",
                    post(add_client_subscription),
                )
                .route(
                    "/admin/subscription/:user_id/:channel",
                    delete(remove_client_subscription),
                )
                .route("/channels/:channel/messages", post(publish))
                .with_state(app_state);

            let addr = format!("0.0.0.0:{}", port);
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let http_server = || async move { axum::serve(listener, router).await };
            println!("HTTP server running on port: {}", port);
            http_server().await?;
            Ok(())
        }
    }
}

#[derive(Clone)]
struct BrokerHttpState {
    pub members: ClientMembership,
}

async fn add_client_subscription(
    Path(user_id): Path<String>,
    Path(channel): Path<String>,
    State(app_state): State<BrokerHttpState>,
) -> impl IntoResponse {
    app_state.members.add_client_subscription(user_id, channel);
    "OK"
}

async fn remove_client_subscription(
    Path(user_id): Path<String>,
    Path(channel): Path<String>,
    State(app_state): State<BrokerHttpState>,
) -> impl IntoResponse {
    app_state
        .members
        .remove_client_subscription(user_id, channel);
    "OK"
}

async fn publish(
    State(app_state): State<BrokerHttpState>,
    Path(channel): Path<String>,
    body: String,
) -> impl IntoResponse {
    let payload = body;
    let payload = Arc::new(payload);
    app_state
        .members
        .with_client_subscriptions(channel.clone(), |client_id| {
            let has_channel = app_state
                .members
                .with_client_channel(client_id.clone(), |sender| {
                    let send_result = sender.send(Arc::clone(&payload));
                    match send_result {
                        Err(e) => anyhow::Result::Err(e.into()),
                        Ok(result) => anyhow::Ok(result),
                    }
                });

            match has_channel {
                Some(Err(_)) => {
                    // Receiver cannot receive messages at the moment
                    // For now, we will assume the client is disconnected
                    // and we will remove it from the list of subscribers
                    //
                    // In the future, we should have a way to wait for its
                    // reconnection
                    app_state
                        .members
                        .remove_client_subscription(client_id.clone(), channel.clone());
                }
                None => {
                    // A client we thought was a subscriber doesn't have
                    // a internal channel to receive messages
                    // As there is no way to communicate to the client
                    // we need to assume the client is disconnected
                    app_state
                        .members
                        .remove_client_subscription(client_id.clone(), channel.clone());
                }
                _ => {}
            }
            Ok(())
        })
        .unwrap();
    "OK"
}

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use futures::StreamExt;
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::admin_commands::{AdminCommand, AdminCommands};

type CHashMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
static LOCK_POISONED_ERROR: &str = "Lock poisoned: This is an unrecoverable error";

#[derive(Debug)]
pub enum BrokerMessage {
    ConnectClient(String, oneshot::Sender<broadcast::Sender<String>>),
    DisconnectClient(String),
    // client, channel
    Subscribe(String, String),
    // client, channel
    Unsubscribe(String, String),
}

/// Builder-pattern for RedisBroker
///
/// # Example
///
/// ```rust
/// # use broadcast_rs::broker::RedisBrokerConfig;
/// # use anyhow::Result;
/// # async fn run() -> Result<()> {
///     let mut broker_config = RedisBrokerConfig::new();
///     broker_config.redis_url = "redis://localhost:6666/".to_string();
///     broker_config.channel_capacity = 100;
///     let mut broker = broker_config.build()?;
///     broker.run().await?;
///     Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
pub struct RedisBrokerConfig {
    pub redis_url: String,
    pub channel_capacity: usize,
}

impl RedisBrokerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> Result<RedisBroker> {
        self.try_into()
    }
}

/// A broker that listens for messages on a redis channel and sends them to clients.
///
/// The flow of the broker is (simplified) as follows:
/// 1. The broker starts and connects to redis.
/// 2. The broker listens for messages on the "global"
/// 3. Clients connect to the broker. The broker creates a new channel for each client.
/// 4. Clients subscribe to the broker's channel by sending a message to the broker. The broker
///   listens for these messages and adds to its internal list of subscriptions.
/// 5. When the broker receives a message, it routes to the appropriate client's channel.
///
/// TODO: Make it generic over message type
/// TODO: Add diagram
pub struct RedisBroker {
    pub redis_client: redis::Client,
    pub channel_capacity: usize,

    /// A sender for the broker to send messages to the clients. The key is the client's id.
    pub senders: CHashMap<String, broadcast::Sender<String>>,

    /// A map of subscriptions. The key is the channel, and the value is a list of client ids.
    pub client_subscriptions: CHashMap<String, HashSet<String>>,

    /// join handlers
    pub subscription_join_handlers: CHashMap<String, tokio::task::JoinHandle<Result<()>>>,

    /// (channel, client_id)
    pub api_tx: mpsc::UnboundedSender<BrokerMessage>,
    pub api_rx: mpsc::UnboundedReceiver<BrokerMessage>,
}

/// Convert a RedisBrokerConfig into a RedisBroker
///
/// # Example
/// ```rust
/// # use broadcast_rs::broker::{RedisBroker, RedisBrokerConfig};
/// # use anyhow::Result;
/// # async fn run() -> Result<()> {
///    let mut broker_config = RedisBrokerConfig::new();
///    broker_config.redis_url = "redis://localhost:6666/".to_string();
///    broker_config.channel_capacity = 100;
///    let broker = RedisBroker::try_from(broker_config.clone())?;
///    // or
///    let broker: RedisBroker = broker_config.try_into()?;
///    # Ok(())
/// # }
/// ```
impl TryFrom<RedisBrokerConfig> for RedisBroker {
    type Error = anyhow::Error;

    fn try_from(config: RedisBrokerConfig) -> std::prelude::v1::Result<Self, Self::Error> {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let redis_client = redis::Client::open(config.redis_url.as_str())?;

        let new_self = RedisBroker {
            channel_capacity: config.channel_capacity,
            api_tx,
            api_rx,
            redis_client,
            senders: Default::default(),
            client_subscriptions: Default::default(),
            subscription_join_handlers: Default::default(),
        };
        Ok(new_self)
    }
}

impl RedisBroker {
    /// Create a new RedisBroker with default configuration
    ///
    /// ```ignore
    /// # use broadcast_rs::broker::RedisBroker;
    /// let broker = RedisBroker::config().build()?;
    /// ```
    pub fn config() -> RedisBrokerConfig {
        RedisBrokerConfig::default()
    }

    /// Create a new API object for this broker
    pub fn api(&self) -> BrokerApi {
        BrokerApi::new(self.api_tx.clone())
    }

    /// Handles `BokerMessage::ConnectClient` messages
    fn handle_connect_client(
        &self,
        client_id: String,
        tx: oneshot::Sender<broadcast::Sender<String>>,
    ) -> Result<()> {
        let (broadcast_tx, _) = broadcast::channel(self.channel_capacity);
        self.senders
            .write()
            .expect(LOCK_POISONED_ERROR)
            .insert(client_id, broadcast_tx.clone());
        tx.send(broadcast_tx).expect("TODO");
        Ok(())
    }

    /// Handles `BokerMessage::DisconnectClient` messages
    fn handle_disconnect_client(&self, client_id: String) -> Result<()> {
        Self::disconnect_client(
            self.client_subscriptions.clone(),
            self.senders.clone(),
            client_id,
        )
    }

    fn disconnect_client(
        client_subscriptions: CHashMap<String, HashSet<String>>,
        senders: CHashMap<String, broadcast::Sender<String>>,
        client_id: String,
    ) -> Result<()> {
        let mut client_subscriptions_guard =
            client_subscriptions.write().expect(LOCK_POISONED_ERROR);

        for i in client_subscriptions_guard.values_mut() {
            i.remove(&client_id);
        }
        let client_tx = senders
            .write()
            .expect(LOCK_POISONED_ERROR)
            .remove(&client_id);

        client_tx.and_then(|tx| tx.send("#DISCONNECTED#".to_string()).ok()); // TODO handle on the other side

        Ok(())
    }

    /// Handles [BokerMessage::Subscribe] messages
    async fn handle_subscribe(&self, client_id: String, channel: String) -> Result<()> {
        tracing::debug!("handle_subscribe({}, {})", client_id, channel);

        let client_subscriptions = self.client_subscriptions.clone();
        let senders = self.senders.clone();

        client_subscriptions
            .clone()
            .write()
            .expect(LOCK_POISONED_ERROR)
            .entry(channel.clone())
            .or_insert(HashSet::new())
            .insert(client_id.clone());

        if let Some(_) = self
            .subscription_join_handlers
            .read()
            .expect(LOCK_POISONED_ERROR)
            .get(&channel)
        {
            return Ok(());
        }

        let inner_client_subscriptions = client_subscriptions.clone();
        let inner_senders = senders.clone();
        let inner_channel = channel.clone();
        let inner_subscription_join_handlers = self.subscription_join_handlers.clone();
        let redis_client = self.redis_client.clone();
        let conn = redis_client.get_tokio_connection().await?;

        let subscription_join_handler = tokio::spawn(async move {
            let mut pubsub = conn.into_pubsub();
            pubsub.subscribe(inner_channel.clone()).await?;
            let mut msg_stream = pubsub.on_message();

            // TODO handle error
            // Serve loop
            loop {
                if let Some(msg) = msg_stream.next().await {
                    let payload: String = msg.get_payload()?;
                    for client_id in inner_client_subscriptions
                        .read()
                        .expect(LOCK_POISONED_ERROR)
                        .get(&inner_channel)
                        .unwrap_or(&HashSet::new())
                    {
                        if let Some(sender) = inner_senders
                            .read()
                            .expect(LOCK_POISONED_ERROR)
                            .get(client_id)
                        {
                            // TODO Stop cloning the payload
                            // TODO maybe I can use `Cow<'static, &str>` instead
                            let send_result = sender.send(payload.clone());

                            // Receiver cannot receive messages at the moment
                            // For now, we will assume the client is disconnected
                            // and we will remove it from the list of subscribers
                            //
                            // In the future, we should have a way to wait for its
                            // reconnection
                            if send_result.is_err() {
                                Self::disconnect_client(
                                    inner_client_subscriptions.clone(),
                                    inner_senders.clone(),
                                    client_id.clone(),
                                )?;
                            }
                        } else {
                            // A client we thought was a subscriber doesn't have
                            // a internal channel to receive messages
                            // As there is no way to communicate to the client
                            // we need to assume the client is disconnected
                            Self::disconnect_client(
                                inner_client_subscriptions.clone(),
                                inner_senders.clone(),
                                client_id.clone(),
                            )?;
                        }
                    }
                } else {
                    tracing::warn!("No more messages from the upstream channel");
                    // Drop stream without losing the msg_stream var
                    msg_stream.collect::<Vec<_>>().await;
                    // If the connection was lost, we need to reconnect
                    // TODO backoff
                    let mut conn = pubsub.into_connection().await;
                    let pong = redis::cmd("PING").query_async::<_, String>(&mut conn).await;
                    if let Err(e) = pong {
                        tracing::error!(
                            "Lost broker's connection to redis. Trying to reconnect ({:?})",
                            e
                        );
                        conn = redis_client.get_tokio_connection().await?;
                        pubsub = conn.into_pubsub();
                        pubsub.subscribe(inner_channel.clone()).await?;
                        msg_stream = pubsub.on_message();
                    } else {
                        // No more messages from the upstream channel and there was no disconnect
                        // Breaking here will allow us to run the tear down code
                        // below
                        break;
                    }
                }
            }

            // Task tear down
            //
            // As we have one task per channel regardless of the number of
            // subscribers, when the task is over we can safely remove all
            // subscribers from the channel, then remove the channel itself
            // from the list of running tasks
            inner_client_subscriptions
                .write()
                .expect(LOCK_POISONED_ERROR)
                .remove_entry(&inner_channel);

            inner_subscription_join_handlers
                .write()
                .expect(LOCK_POISONED_ERROR)
                .remove(&inner_channel);
            Ok::<(), anyhow::Error>(())
        });

        self.subscription_join_handlers
            .write()
            .expect(LOCK_POISONED_ERROR)
            .insert(channel, subscription_join_handler);
        Ok(())
    }

    /// Handles [BrokerMessage::Unsubscribe] messages
    fn handle_unsubscribe(&self, client_id: String, channel: String) -> Result<()> {
        self.client_subscriptions
            .write()
            .expect(LOCK_POISONED_ERROR)
            .entry(channel.clone())
            .or_insert(HashSet::new())
            .remove(&client_id);
        Ok(())
    }

    /// List to messages from Redis on the 'admin' channel
    /// This channel receives commands to control the broker's behavior
    ///
    /// # Commands
    ///
    /// - subscribe:client_id:channel
    /// - unsubscribe:client_id:channel
    #[async_recursion::async_recursion]
    async fn serve_admin_commands(
        redis_client: redis::Client,
        api_tx: mpsc::UnboundedSender<BrokerMessage>,
    ) -> Result<()> {
        let conn = redis_client.get_tokio_connection().await?;

        let mut pubsub = conn.into_pubsub();
        pubsub.subscribe("admin").await?;
        let mut msg_stream = pubsub.on_message();
        tracing::error!("Waiting for new messages on admin channel");

        while let Some(msg) = msg_stream.next().await {
            let payload: String = msg.get_payload()?;
            let admin_command = AdminCommand::try_from(payload)?;
            match &admin_command.command {
                AdminCommands::Subscribe(client_id, channel) => {
                    api_tx.send(BrokerMessage::Subscribe(
                        client_id.to_string(),
                        channel.to_string(),
                    ))?;
                }
                AdminCommands::Unsubscribe(client_id, channel) => {
                    api_tx.send(BrokerMessage::Unsubscribe(
                        client_id.to_string(),
                        channel.to_string(),
                    ))?;
                }
                AdminCommands::Shutdown => {
                    tracing::info!("Shutting down admin commands server");
                    break;
                }
                command => {
                    tracing::error!("Command not implemented: {:?}", command);
                }
            }
        }
        drop(msg_stream);

        // Test if terminated because the connection was lost
        let mut conn = pubsub.into_connection().await;
        let pong = redis::cmd("PING").query_async::<_, String>(&mut conn).await;

        if let Err(e) = pong {
            tracing::error!(
                "Lost admin connection to redis. Trying to reconnect ({:?})",
                e
            );
            return Self::serve_admin_commands(redis_client, api_tx).await;
        }
        Ok(())
    }

    /// Listen to commands from the API channel and handle each command
    async fn serve_broker_api(&mut self) -> Result<()> {
        while let Some(broker_message) = self.api_rx.recv().await {
            match broker_message {
                BrokerMessage::ConnectClient(client_id, tx) => {
                    self.handle_connect_client(client_id, tx)?;
                }
                BrokerMessage::DisconnectClient(client_id) => {
                    self.handle_disconnect_client(client_id)?;
                }
                BrokerMessage::Subscribe(client_id, channel) => {
                    self.handle_subscribe(client_id, channel).await?;
                }
                BrokerMessage::Unsubscribe(client_id, channel) => {
                    self.handle_unsubscribe(client_id, channel)?;
                }
            };
        }
        Ok(())
    }

    /// Runs all tasks required for the broker to function concurrently
    ///
    /// This includes:
    /// - Listening for messages on the 'admin' channel
    /// - Listening for messages on the API channel
    ///
    /// If any of the tasks fail, the entire function will return an error
    pub async fn run(&mut self) -> Result<()> {
        let redis_client = self.redis_client.clone();
        let api_tx = self.api_tx.clone();
        let admin_commands = Self::serve_admin_commands(redis_client, api_tx);
        let broker_api = self.serve_broker_api();
        let result = tokio::select! {
            r = admin_commands => {
                tracing::error!("Admin commands server finished: {:?}", r);
                r
            }
            r = broker_api => {
                tracing::error!("Broker API finished: {:?}", r);
                r
            }
        };
        result
    }
}

/// Decouple RedisBroker's controls from the actual broker
///
/// This is useful to avoid ownership issues when using the broker in a web server
#[derive(Clone)]
pub struct BrokerApi {
    pub broker_tx: mpsc::UnboundedSender<BrokerMessage>,
}

impl BrokerApi {
    pub fn new(broker_tx: mpsc::UnboundedSender<BrokerMessage>) -> Self {
        Self { broker_tx }
    }

    pub async fn connect_client(&self, client_id: String) -> Result<broadcast::Sender<String>> {
        let (tx, rx) = oneshot::channel();
        self.broker_tx
            .send(BrokerMessage::ConnectClient(client_id, tx))?;
        let broadcast_rx = rx.await?;
        Ok(broadcast_rx)
    }

    pub async fn disconnect_client(&self, client_id: String) -> Result<()> {
        self.broker_tx
            .send(BrokerMessage::DisconnectClient(client_id))?;
        Ok(())
    }

    /// TODO
    pub async fn subscribe(&self, client_id: String, channel: String) -> Result<()> {
        let msg = BrokerMessage::Subscribe(client_id, channel);
        self.broker_tx.send(msg)?;
        Ok(())
    }

    pub async fn unsubscribe(&self, client_id: String, channel: String) -> Result<()> {
        let msg = BrokerMessage::Unsubscribe(client_id, channel);
        self.broker_tx.send(msg)?;
        Ok(())
    }
}

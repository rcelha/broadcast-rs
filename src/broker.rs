use std::collections::{HashMap, HashSet};

use anyhow::Result;
use futures::StreamExt;
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, mpsc, oneshot};

type CHashMap<K, V> = Arc<RwLock<HashMap<K, V>>>;

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
        RedisBroker::from_config(self)
    }
}

/// A broker that listens for messages on a redis channel and sends them to clients.
///
/// The flow of the broker is (simplified) as follows:
/// 1. The broker starts and connects to redis.
/// 2. The broker listens for messages on the "global" channel with [[RedisBroker::serve_broker]]
/// 3. Clients connect to the broker. The broker creates a new channel for each client.
/// 4. Clients subscribe to the broker's channel by sending a message to the broker. The broker
///   listens for these messages and adds to its internal list of subscriptions.
/// 5. When the broker receives a message, it routes to the appropriate client's channel.
///
/// TODO: Make it generic over message type
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

    // TODO move it to `impl From` trait
    pub fn from_config(config: RedisBrokerConfig) -> Result<Self> {
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

    /// Create a new API object for this broker
    pub fn api(&self) -> BrokerApi {
        BrokerApi::new(self.api_tx.clone())
    }

    async fn handle_connect_client(
        &self,
        client_id: String,
        tx: oneshot::Sender<broadcast::Sender<String>>,
    ) -> Result<()> {
        let (broadcast_tx, _) = broadcast::channel(self.channel_capacity);
        self.senders
            .write()
            .unwrap()
            .insert(client_id, broadcast_tx.clone());
        tx.send(broadcast_tx).expect("TODO");
        Ok(())
    }

    async fn handle_disconnect_client(&self, client_id: String) -> Result<()> {
        let mut client_subscriptions = self.client_subscriptions.write().unwrap();
        for i in client_subscriptions.values_mut() {
            i.remove(&client_id);
        }
        let client_tx = self.senders.write().unwrap().remove(&client_id);
        client_tx.and_then(|tx| tx.send("#DISCONNECTED#".to_string()).ok()); // TODO handle on the other side
        Ok(())
    }

    async fn handle_subscribe(&self, client_id: String, channel: String) -> Result<()> {
        let conn = self.redis_client.get_tokio_connection().await?;

        let client_subscriptions = self.client_subscriptions.clone();
        let senders = self.senders.clone();

        let mut pubsub = conn.into_pubsub();
        pubsub.subscribe(channel.clone()).await?;

        client_subscriptions
            .clone()
            .write()
            .unwrap()
            .entry(channel.clone())
            .or_insert(HashSet::new())
            .insert(client_id.clone());

        if let Some(_) = self
            .subscription_join_handlers
            .read()
            .unwrap()
            .get(&channel)
        {
            return Ok(());
        }

        let inner_client_subscriptions = client_subscriptions.clone();
        let inner_senders = senders.clone();
        let inner_channel = channel.clone();

        let subscription_join_handler = tokio::spawn(async move {
            // TODO handle error
            // Serve loop
            loop {
                let mut msg_stream = pubsub.on_message();
                if let Some(msg) = msg_stream.next().await {
                    let payload: String = msg.get_payload()?;
                    for client_id in inner_client_subscriptions
                        .read()
                        .unwrap()
                        .get(&inner_channel)
                        .unwrap_or(&HashSet::new())
                    {
                        if let Some(sender) = inner_senders.read().unwrap().get(client_id) {
                            // TODO Stop cloning the payload
                            // TODO maybe I can use `Cow<'static, &str>` instead
                            sender.send(payload.clone()).unwrap();
                        }
                    }
                } else {
                    break;
                }
            }

            // serve tear down
            let mut client_subscriptions_guard = inner_client_subscriptions.write().unwrap();

            // Remove current client from the channel
            client_subscriptions_guard
                .entry(inner_channel.clone())
                .or_insert(HashSet::new())
                .remove(&client_id);

            // delete channel if empty
            if let Some(clients) = client_subscriptions_guard.get(&inner_channel) {
                if clients.is_empty() {
                    client_subscriptions_guard.remove(&inner_channel);
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        self.subscription_join_handlers
            .write()
            .unwrap()
            .insert(channel, subscription_join_handler);

        Ok(())
    }

    async fn handle_unsubscribe(&self, client_id: String, channel: String) -> Result<()> {
        self.client_subscriptions
            .write()
            .unwrap()
            .entry(channel.clone())
            .or_insert(HashSet::new())
            .remove(&client_id);
        Ok(())
    }

    async fn serve_admin_commands(
        redis_client: redis::Client,
        api_tx: mpsc::UnboundedSender<BrokerMessage>,
    ) -> Result<()> {
        let conn = redis_client.get_tokio_connection().await?;
        let mut pubsub = conn.into_pubsub();
        pubsub.subscribe("admin").await?;
        let mut msg_stream = pubsub.on_message();
        while let Some(msg) = msg_stream.next().await {
            let payload: String = msg.get_payload()?;
            let mut iter = payload.split(':');
            let command = iter.next().ok_or(anyhow::anyhow!("No command"))?;
            match command {
                "subscribe" => {
                    let client_id = iter.next().ok_or(anyhow::anyhow!("No client_id"))?;
                    let channel = iter.next().ok_or(anyhow::anyhow!("No channel name"))?;
                    api_tx.send(BrokerMessage::Subscribe(
                        client_id.to_string(),
                        channel.to_string(),
                    ))?;
                }
                "unsubscribe" => {
                    let client_id = iter.next().ok_or(anyhow::anyhow!("No client_id"))?;
                    let channel = iter.next().ok_or(anyhow::anyhow!("No channel name"))?;
                    api_tx.send(BrokerMessage::Unsubscribe(
                        client_id.to_string(),
                        channel.to_string(),
                    ))?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub async fn serve_broker_api(&mut self) -> Result<()> {
        while let Some(broker_message) = self.api_rx.recv().await {
            match broker_message {
                BrokerMessage::ConnectClient(client_id, tx) => {
                    self.handle_connect_client(client_id, tx).await?;
                }
                BrokerMessage::DisconnectClient(client_id) => {
                    self.handle_disconnect_client(client_id).await?;
                }
                BrokerMessage::Subscribe(client_id, channel) => {
                    self.handle_subscribe(client_id, channel).await?;
                }
                BrokerMessage::Unsubscribe(client_id, channel) => {
                    self.handle_unsubscribe(client_id, channel).await?;
                }
            };
        }
        Ok(())
    }

    /// TODO
    pub async fn run(&mut self) -> Result<()> {
        let redis_client = self.redis_client.clone();
        let api_tx = self.api_tx.clone();
        let admin_commands = Self::serve_admin_commands(redis_client, api_tx);
        let broker_api = self.serve_broker_api();
        tokio::select! {
            r = admin_commands => { r}
            r = broker_api => {r}
        }?;
        Ok(())
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

use std::collections::HashMap;

use anyhow::Result;
use futures::{Future, StreamExt};
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    admin_commands::{AdminCommand, AdminCommands},
    broker::Broker,
    broker_api::BrokerCommand,
    client_membership::ClientMembership,
};

type CHashMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
type ClientMessage = Arc<String>;

static LOCK_POISONED_ERROR: &str = "Lock poisoned: This is an unrecoverable error";

/// Builder-pattern for RedisBroker
///
/// # Example
///
/// ```rust
/// # use broadcast_rs::prelude::*;
/// # use anyhow::Result;
/// # async fn run() -> Result<()> {
///     let mut broker_config = RedisBrokerConfig::new();
///     broker_config.redis_url = "redis://localhost:6666/".to_string();
///     broker_config.channel_capacity = 100;
///     let mut broker: RedisBroker = broker_config.build()?;
///     // let mut broker = broker_config.build()?;
///     run_broker(&mut broker).await?;
///     Ok(())
/// # }
/// ```
#[derive(Clone, Default, Debug)]
pub struct RedisBrokerConfig {
    pub redis_url: String,
    pub channel_capacity: usize,
}

impl RedisBrokerConfig {
    pub fn new() -> Self {
        Self::default()
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
    pub members: ClientMembership,

    /// join handlers
    pub subscription_join_handlers: CHashMap<String, tokio::task::JoinHandle<Result<()>>>,

    /// (channel, client_id)
    pub api_tx: mpsc::UnboundedSender<BrokerCommand>,
    pub api_rx: mpsc::UnboundedReceiver<BrokerCommand>,
}

/// Convert a RedisBrokerConfig into a RedisBroker
///
/// # Example
/// ```rust
/// # use broadcast_rs::prelude::*;
/// # use anyhow::Result;
/// # async fn run() -> Result<()> {
///    let mut broker_config = RedisBrokerConfig::new();
///    broker_config.redis_url = "redis://localhost:6666/".to_string();
///    broker_config.channel_capacity = 100;
///    let broker = RedisBroker::try_from(broker_config.clone())?;
///    // or
///    let broker: RedisBroker = broker_config.clone().try_into()?;
///    // or
///    let broker: RedisBroker = broker_config.clone().build()?;
///    # Ok(())
/// # }
/// ```
impl TryFrom<RedisBrokerConfig> for RedisBroker {
    type Error = anyhow::Error;

    fn try_from(val: RedisBrokerConfig) -> std::prelude::v1::Result<Self, Self::Error> {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let redis_client = redis::Client::open(val.redis_url.as_str())?;

        Ok(RedisBroker {
            channel_capacity: val.channel_capacity,
            api_tx,
            api_rx,
            redis_client,
            members: Default::default(),
            subscription_join_handlers: Default::default(),
        })
    }
}

impl RedisBroker {
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
        api_tx: mpsc::UnboundedSender<BrokerCommand>,
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
                    api_tx.send(BrokerCommand::Subscribe(
                        client_id.to_string(),
                        channel.to_string(),
                    ))?;
                }
                AdminCommands::Unsubscribe(client_id, channel) => {
                    api_tx.send(BrokerCommand::Unsubscribe(
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
}
impl Broker for RedisBroker {
    type Config = RedisBrokerConfig;

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
            .add_client(client_id.clone(), tx, self.channel_capacity)?;
        Ok(())
    }

    async fn handle_disconnect_client(&self, client_id: String) -> Result<()> {
        self.members.remove_client(client_id);
        Ok(())
    }

    async fn handle_subscribe(&self, client_id: String, channel: String) -> Result<()> {
        tracing::debug!("handle_subscribe({}, {})", client_id, channel);

        self.members
            .add_client_subscription(client_id.clone(), channel.clone());

        if self
            .subscription_join_handlers
            .read()
            .expect(LOCK_POISONED_ERROR)
            .get(&channel)
            .is_some()
        {
            return Ok(());
        }

        let inner_members = self.members.clone();
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
                    let payload = Arc::new(payload);

                    inner_members.with_client_subscriptions(
                        inner_channel.clone(),
                        |client_id| {
                            let has_channel =
                                inner_members.with_client_channel(client_id.clone(), |sender| {
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
                                    inner_members.remove_client_subscription(
                                        client_id.clone(),
                                        inner_channel.clone(),
                                    );
                                }
                                None => {
                                    // A client we thought was a subscriber doesn't have
                                    // a internal channel to receive messages
                                    // As there is no way to communicate to the client
                                    // we need to assume the client is disconnected
                                    inner_members.remove_client_subscription(
                                        client_id.clone(),
                                        inner_channel.clone(),
                                    );
                                }
                                _ => {}
                            }
                            Ok(())
                        },
                    )?;
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
            inner_members.remove_subscription(inner_channel.clone());
            inner_subscription_join_handlers
                .write()
                .expect(LOCK_POISONED_ERROR)
                .remove(&inner_channel);
            anyhow::Ok(())
        });

        self.subscription_join_handlers
            .write()
            .expect(LOCK_POISONED_ERROR)
            .insert(channel, subscription_join_handler);
        Ok(())
    }

    async fn handle_unsubscribe(&self, client_id: String, channel: String) -> Result<()> {
        self.members.remove_client_subscription(client_id, channel);
        Ok(())
    }

    fn run_backend_server<'broker, 'fut: 'broker>(
        &'broker self,
    ) -> impl Future<Output = Result<()>> + Send + 'fut {
        let redis_client = self.redis_client.clone();
        let api_tx = self.api_tx.clone();

        Self::serve_admin_commands(redis_client, api_tx)
    }
}

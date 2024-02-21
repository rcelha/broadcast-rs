use anyhow::Result;
use either::Either;
use futures::Future;
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::broker_api::{BrokerApi, BrokerCommand};

type ClientMessage = Arc<String>;

/// Builder-pattern for a Broker's implementation
///
/// Every broker should also have a `BrokerConfig` implementation.
///
/// There is an auto implementation for every type that implements `Broker`,
/// as long as the Broker has a `Config` type.
pub trait BrokerConfig<T: Broker + TryFrom<Self>>: Clone + Default {
    fn build(self) -> Result<T> {
        let target = T::try_from(self).or(Err(anyhow::anyhow!("Failed to convert config")))?;
        anyhow::Ok(target)
    }
}

impl<T, C> BrokerConfig<T> for C
where
    T: Broker<Config = C> + TryFrom<Self>,
    C: Clone + Default,
{
}

/// A broker is a message broker that can handle multiple clients
///
/// This trait expresses the minimal requirements for a broker's backend implementation,
/// and it also implements automatically its interaction with [[BrokerApi]]
pub trait Broker: Sized + Send + TryFrom<Self::Config> {
    type Config: BrokerConfig<Self>;

    /// Getter for the [BrokerApi] sender channel
    fn api_tx(&self) -> &mpsc::UnboundedSender<BrokerCommand>;

    /// Getter for the [BrokerApi] receiver channel
    fn api_rx(&mut self) -> &mut mpsc::UnboundedReceiver<BrokerCommand>;

    /// Handles [BrokerCommand::ConnectClient] messages
    fn handle_connect_client(
        &self,
        client_id: String,
        tx: oneshot::Sender<broadcast::Sender<ClientMessage>>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Handles [BrokerCommand::DisconnectClient] messages
    fn handle_disconnect_client(
        &self,
        client_id: String,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Handles [BrokerCommand::Subscribe] messages
    fn handle_subscribe(
        &self,
        client_id: String,
        channel: String,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Handles [BrokerCommand::Unsubscribe] messages
    fn handle_unsubscribe(
        &self,
        client_id: String,
        channel: String,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Async function to setup and run any taks required for the broker to function
    /// with the backend of your choice
    ///
    /// It is important to understand its lifetimes: The future it returns needs to
    /// outlive `self`. That means your server loop cannot depend on `self`, but it
    /// needs to rely on cloned values
    ///
    /// # Example
    ///
    /// ```ignore
    /// impl Broker for MyBroker {
    ///
    /// // (...)
    ///
    ///  fn run_backend_server<'a, 'b: 'a>(&'a self) -> impl Future<Output = Result<()>> + Send + 'b {
    ///         let inner_field = self.field.clone();
    ///         async {
    ///             // This block can use `inner_field` but not `self`
    ///         }
    ///     }
    ///
    /// // (...)
    ///
    /// }
    /// ```
    fn run_backend_server<'broker, 'fut>(
        &'broker self,
    ) -> impl Future<Output = Result<()>> + Send + 'fut
    where
        'fut: 'broker;

    /// Creates a new config instance with default values
    /// This is handy so one don't need to know the actual config type
    ///
    /// ```ignore
    /// let broker = MyBroker::config().build()?;
    /// ```
    fn config() -> Self::Config {
        Self::Config::default()
    }

    /// Creates a new broker api instance to interact with the broker
    fn api(&self) -> BrokerApi {
        BrokerApi::new(self.api_tx().clone())
    }

    /// Listens to messages coming thought [BrokerApi] and route each message
    /// to the appropriate handler
    fn run_broker_api_server(&mut self) -> impl Future<Output = Result<()>> + Send {
        async {
            while let Some(broker_message) = self.api_rx().recv().await {
                match broker_message {
                    BrokerCommand::ConnectClient(client_id, tx) => {
                        self.handle_connect_client(client_id, tx).await?;
                    }
                    BrokerCommand::DisconnectClient(client_id) => {
                        self.handle_disconnect_client(client_id).await?;
                    }
                    BrokerCommand::Subscribe(client_id, channel) => {
                        self.handle_subscribe(client_id, channel).await?;
                    }
                    BrokerCommand::Unsubscribe(client_id, channel) => {
                        self.handle_unsubscribe(client_id, channel).await?;
                    }
                };
            }
            Ok(())
        }
    }
}

/// Runs all tasks required for the broker to function concurrently
///
/// This includes:
/// - Listening for messages on the 'admin' channel
/// - Listening for messages on the API channel
///
/// If any of the tasks fail, the entire function will return an error
///
/// **WARNING**:
///
/// This function is supposed to be part of the [Broker] trait, but it is
/// not possible at the moment due this
/// [issue](https://github.com/rust-lang/rust/issues/100013) might be a blocker
pub async fn run_broker<T: Broker>(broker: &mut T) -> Result<()> {
    let admin_commands = broker.run_backend_server();
    let broker_api = broker.run_broker_api_server();
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

/// A broker that can be configured to use one of two different brokers

pub struct EitherConfig<B1, B2, C1, C2>(pub either::Either<C1, C2>, PhantomData<(B1, B2)>)
where
    B1: Broker<Config = C1> + TryFrom<C1>,
    B2: Broker<Config = C2> + TryFrom<C2>,
    C1: BrokerConfig<B1> + Clone,
    C2: BrokerConfig<B2> + Clone;

impl<B1, B2, C1, C2> Clone for EitherConfig<B1, B2, C1, C2>
where
    B1: Broker<Config = C1> + TryFrom<C1>,
    B2: Broker<Config = C2> + TryFrom<C2>,
    C1: BrokerConfig<B1> + Clone,
    C2: BrokerConfig<B2> + Clone,
{
    fn clone(&self) -> Self {
        EitherConfig(self.0.clone(), PhantomData {})
    }
}

impl<B1, B2, C1, C2> Default for EitherConfig<B1, B2, C1, C2>
where
    B1: Broker<Config = C1> + TryFrom<C1>,
    B2: Broker<Config = C2> + TryFrom<C2>,
    C1: BrokerConfig<B1> + Clone,
    C2: BrokerConfig<B2> + Clone,
{
    fn default() -> Self {
        EitherConfig(either::Either::Left(C1::default()), PhantomData {})
    }
}

impl<B1, B2, C1, C2> TryFrom<EitherConfig<B1, B2, C1, C2>> for Either<B1, B2>
where
    B1: Broker<Config = C1> + TryFrom<C1>,
    B2: Broker<Config = C2> + TryFrom<C2>,
    C1: BrokerConfig<B1> + Clone,
    C2: BrokerConfig<B2> + Clone,
{
    type Error = anyhow::Error;

    fn try_from(val: EitherConfig<B1, B2, C1, C2>) -> std::prelude::v1::Result<Self, Self::Error> {
        match val.0 {
            either::Either::Left(c) => Ok(Either::Left(c.build()?)),
            either::Either::Right(c) => Ok(Either::Right(c.build()?)),
        }
    }
}

impl<B1, B2, C1, C2> Broker for Either<B1, B2>
where
    B1: Broker<Config = C1> + Sync + TryFrom<C1>,
    B2: Broker<Config = C2> + Sync + TryFrom<C2>,
    C1: BrokerConfig<B1> + Clone,
    C2: BrokerConfig<B2> + Clone,
{
    type Config = EitherConfig<B1, B2, C1, C2>;

    fn api_tx(&self) -> &tokio::sync::mpsc::UnboundedSender<crate::broker_api::BrokerCommand> {
        match self {
            Either::Left(b) => b.api_tx(),
            Either::Right(b) => b.api_tx(),
        }
    }

    fn api_rx(
        &mut self,
    ) -> &mut tokio::sync::mpsc::UnboundedReceiver<crate::broker_api::BrokerCommand> {
        match self {
            Either::Left(b) => b.api_rx(),
            Either::Right(b) => b.api_rx(),
        }
    }

    async fn handle_connect_client(
        &self,
        client_id: String,
        tx: oneshot::Sender<broadcast::Sender<ClientMessage>>,
    ) -> Result<()> {
        match self {
            Either::Left(b) => b.handle_connect_client(client_id, tx).await,
            Either::Right(b) => b.handle_connect_client(client_id, tx).await,
        }
    }

    /// Handles [BrokerCommand::DisconnectClient] messages
    async fn handle_disconnect_client(&self, client_id: String) -> Result<()> {
        match self {
            Either::Left(b) => b.handle_disconnect_client(client_id).await,
            Either::Right(b) => b.handle_disconnect_client(client_id).await,
        }
    }

    async fn handle_subscribe(&self, client_id: String, channel: String) -> Result<()> {
        match self {
            Either::Left(b) => b.handle_subscribe(client_id, channel).await,
            Either::Right(b) => b.handle_subscribe(client_id, channel).await,
        }
    }

    /// Handles [BrokerCommand::Unsubscribe] messages
    async fn handle_unsubscribe(&self, client_id: String, channel: String) -> Result<()> {
        match self {
            Either::Left(b) => b.handle_unsubscribe(client_id, channel).await,
            Either::Right(b) => b.handle_unsubscribe(client_id, channel).await,
        }
    }

    fn run_backend_server<'broker, 'fut>(
        &'broker self,
    ) -> impl Future<Output = Result<()>> + Send + 'fut
    where
        'fut: 'broker,
    {
        let fut = match self {
            Either::Left(b) => Either::Left(b.run_backend_server()),
            Either::Right(b) => Either::Right(b.run_backend_server()),
        };
        fut
    }
}

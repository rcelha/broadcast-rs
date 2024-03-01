use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};

type ClientMessage = Arc<String>;

/// **TODO**
#[derive(Debug)]
pub enum BrokerCommand {
    /// ConnectClient(client, response_channel)
    ConnectClient(String, oneshot::Sender<broadcast::Sender<ClientMessage>>),
    /// Subscribe(client)
    DisconnectClient(String),
    /// Subscribe(client, channel)
    Subscribe(String, String),
    /// Unubscribe(client, channel)
    Unsubscribe(String, String),
}

/// Decouple Broker's controls from the actual broker
///
/// This is useful to avoid ownership issues when using the broker in a web server
#[derive(Clone)]
pub struct BrokerApi {
    pub broker_tx: mpsc::UnboundedSender<BrokerCommand>,
}

impl BrokerApi {
    pub fn new(broker_tx: mpsc::UnboundedSender<BrokerCommand>) -> Self {
        Self { broker_tx }
    }

    pub async fn connect_client(
        &self,
        client_id: String,
    ) -> Result<broadcast::Sender<ClientMessage>> {
        let (tx, rx) = oneshot::channel();
        self.broker_tx
            .send(BrokerCommand::ConnectClient(client_id, tx))?;
        let broadcast_rx = rx.await?;
        Ok(broadcast_rx)
    }

    pub async fn disconnect_client(&self, client_id: String) -> Result<()> {
        self.broker_tx
            .send(BrokerCommand::DisconnectClient(client_id))?;
        Ok(())
    }

    /// TODO
    pub async fn subscribe(&self, client_id: String, channel: String) -> Result<()> {
        let msg = BrokerCommand::Subscribe(client_id, channel);
        self.broker_tx.send(msg)?;
        Ok(())
    }

    pub async fn unsubscribe(&self, client_id: String, channel: String) -> Result<()> {
        let msg = BrokerCommand::Unsubscribe(client_id, channel);
        self.broker_tx.send(msg)?;
        Ok(())
    }
}

use anyhow::Result;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tokio::sync::{broadcast, oneshot};

type ClientMessage = Arc<String>;
type CHashMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
static LOCK_POISONED_ERROR: &str = "Lock poisoned: This is an unrecoverable error";

#[derive(Clone, Default)]
pub struct ClientMembership {
    /// A sender for the broker to send messages to the clients. The key is the client's id.
    pub senders: CHashMap<String, broadcast::Sender<ClientMessage>>,

    /// A map of subscriptions. The key is the channel name, and the value is a list of client ids.
    pub client_subscriptions: CHashMap<String, HashSet<String>>,
}

impl ClientMembership {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a new client and its communication channel
    ///
    /// TODO:
    ///     it might be better to return the broadcast channel's sender and send it through the
    ///     API channel instead of returning a Result
    pub fn add_client(
        &self,
        client_id: String,
        tx: oneshot::Sender<broadcast::Sender<ClientMessage>>,
        channel_capacity: usize,
    ) -> Result<()> {
        let (broadcast_tx, _) = broadcast::channel(channel_capacity);
        self.senders
            .write()
            .expect(LOCK_POISONED_ERROR)
            .insert(client_id, broadcast_tx.clone());

        match tx.send(broadcast_tx) {
            Err(_) => Err(anyhow::anyhow!(
                "Failed to send broadcast channel to client. \
                The receiver sent by the API doesnt have has dropped"
            )),
            Ok(_) => anyhow::Ok(()),
        }
    }

    /// Removes a client and its communication channel
    /// This returns an optional broadcast channel's sender that can be used to send a message to the client
    pub fn remove_client(&self, client_id: String) -> Option<broadcast::Sender<ClientMessage>> {
        let mut client_subscriptions_guard = self
            .client_subscriptions
            .write()
            .expect(LOCK_POISONED_ERROR);

        for i in client_subscriptions_guard.values_mut() {
            i.remove(&client_id);
        }
        let client_tx = self
            .senders
            .write()
            .expect(LOCK_POISONED_ERROR)
            .remove(&client_id);

        client_tx
    }

    /// Executes a function with the client's communication channel
    ///
    /// It returns an `Some` if the client is registered, and `None` otherwise
    pub fn with_client_channel<T>(
        &self,
        client_id: String,
        f: impl FnOnce(&broadcast::Sender<ClientMessage>) -> Result<T>,
    ) -> Option<Result<T>> {
        let senders_guard = self.senders.read().expect(LOCK_POISONED_ERROR);
        senders_guard.get(&client_id).map(f)
    }

    /// Inserts a new subscription for a client
    ///
    /// TODO what if the client is not registered?
    pub fn add_client_subscription(&self, client_id: String, channel: String) {
        let client_subscriptions = self.client_subscriptions.clone();
        client_subscriptions
            .clone()
            .write()
            .expect(LOCK_POISONED_ERROR)
            .entry(channel.clone())
            .or_default()
            .insert(client_id.clone());
    }

    /// Removes a channel's subscription for a client
    pub fn remove_client_subscription(&self, client_id: String, channel: String) {
        self.client_subscriptions
            .write()
            .expect(LOCK_POISONED_ERROR)
            .entry(channel.clone())
            .or_default()
            .remove(&client_id);
    }

    /// Removes all subscriptions on a given channel
    pub fn remove_subscription(&self, channel: String) {
        self.client_subscriptions
            .write()
            .expect(LOCK_POISONED_ERROR)
            .remove(&channel);
    }

    /// Executes a function for each client subscribed to a channel
    /// TODO
    pub fn with_client_subscriptions<T>(
        &self,
        channel: String,
        f: impl Fn(&String) -> Result<T>,
    ) -> Result<()> {
        let subscription_guard = self.client_subscriptions.read().expect(LOCK_POISONED_ERROR);
        let client_ids = subscription_guard.get(&channel);

        if let Some(client_ids) = client_ids {
            for client_id in client_ids {
                f(client_id)?;
            }
        }
        Ok(())
    }
}

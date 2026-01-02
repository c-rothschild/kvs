use tokio::sync::{oneshot, mspc};
use std::path::PathBuf;
use kvs::error::Result;
use kvs::store::Store;
// Messages that clients can send to the store actor
pub enum StoreMessage {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    Get {
        key: Vec<u8>,
        respond_to: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    Del {
        key: Vec<u8>,
        respond_to: oneshot::Sender<Result<bool>>,
    },
}

pub struct StoreActor {
    receiver: mspc::Receiver<StoreMessage>,
    store: Store,
}

impl StoreActor {
    pub fn new(receiver: mspc::Receiver<StoreMessage>, store: Store) -> Self {
        Self {receiver, store}
    }
    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                StoreMessage::Set { key, value, respond_to } => {
                    let result = self.store.set(&key, &value);
                    let _ = respond_to.send(result);
                },
                StoreMessage::Get { key, respond_to } => {
                    let result = self.store.get(&key);
                    let response = Ok(result.map(|s| s.to_vec()));
                    let _ = respond_to.send(response);
                },
                StoreMessage::Del { key, respond_to } => {
                    let result = self.store.del(&key);
                    let _ = respond_to.send(result);
                }
            }
        }
    }
}

// handle for clients
#[derive(Clone)]
pub struct StoreHandle {
    sender: mspc::Sender<StoreMessage>,
}

impl StoreHandle {
    pub fn new(sender: mspc::Sender<StoreMessage>) {
        Self { sender }
    }
    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let msg = StoreMessage::Set {
            key,
            value,
            respond_to: tx,
        };

        // should I change these error types?
        self.sender.send(msg).await
            .map_err(|_| StoreError::StoreClosed { msg: "actor closed".into() })?;

        rx.await
            .map_err(|_| StoreError::StoreClosed { msg: "response channel closed".into() })?
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        let msg = StoreMessage::Get {
            key,
            respond_to: tx,
        };

        // should I change these error types?
        self.sender.send(msg).await
            .map_err(|_| StoreError::StoreClosed { msg: "actor closed".into() })?;

        rx.await
            .map_err(|_| StoreError::StoreClosed { msg: "response channel closed".into() })?
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        let msg = StoreMessage::Del {
            key,
            respond_to: tx,
        };

        // should I change these error types?
        self.sender.send(msg).await
            .map_err(|_| StoreError::StoreClosed { msg: "actor closed".into() })?;

        rx.await
            .map_err(|_| StoreError::StoreClosed { msg: "response channel closed".into() })?
    }
}
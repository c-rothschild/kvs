use tokio::sync::oneshot;
use tokio::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc;
use crate::config::SnapshotMeta;
use crate::error::{Result, StoreError};
use crate::store::Store;
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
    Scan {
        prefix: Option<String>,
        respond_to: oneshot::Sender<Result<Vec<String>>>,
    },
    Snapshot {
        log_path: PathBuf,
        base_dir: PathBuf,
        respond_to: oneshot::Sender<Result<SnapshotMeta>>,

    },
}

pub struct StoreActor {
    receiver: mpsc::Receiver<StoreMessage>,
    store: Store,
}

impl StoreActor {
    pub fn new(receiver: mpsc::Receiver<StoreMessage>, store: Store) -> Self {
        Self {receiver, store}
    }
    pub fn run(mut self) {
        // runs in a blocking thread
        while let Ok(msg) = self.receiver.recv() {
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
                },
                StoreMessage::Scan { prefix, respond_to } => {
                    let result = self.store.scan_prefix_str(prefix.as_deref());
                    let _ = respond_to.send(Ok(result));
                }
                StoreMessage::Snapshot { log_path, base_dir, respond_to } => {
                    let result = self.store.create_snapshot(&log_path, &base_dir);
                    let _ = respond_to.send(result);
                }
            }
        }
    }
}

// handle for clients
#[derive(Clone)]
pub struct StoreHandle {
    sender: mpsc::Sender<StoreMessage>,
}

impl StoreHandle {
    pub fn new(sender: mpsc::Sender<StoreMessage>) -> StoreHandle {
        Self { sender }
    }
    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let msg = StoreMessage::Set {
            key,
            value,
            respond_to: tx,
        };

        self.sender.send(msg)
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

        self.sender.send(msg)
            .map_err(|_| StoreError::StoreClosed { msg: "actor closed".into() })?;

        rx.await
            .map_err(|_| StoreError::StoreClosed { msg: "response channel closed".into() })?
    }

    pub async fn del(&self, key: Vec<u8>) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        let msg = StoreMessage::Del {
            key,
            respond_to: tx,
        };

        self.sender.send(msg)
            .map_err(|_| StoreError::StoreClosed { msg: "actor closed".into() })?;

        rx.await
            .map_err(|_| StoreError::StoreClosed { msg: "response channel closed".into() })?
    }

    pub async fn scan(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let (tx, rx) = oneshot::channel();
        let msg = StoreMessage::Scan {
            prefix: prefix.map(|s| s.to_string()),
            respond_to: tx,
        };
        self.sender.send(msg)
            .map_err(|_| StoreError::StoreClosed { msg: "actor closed".into() })?;

        rx.await
            .map_err(|_| StoreError::StoreClosed { msg: "response channel closed".into() })?
    }

    pub async fn snapshot(&self, log_path: PathBuf, base_dir: PathBuf) -> Result<SnapshotMeta> {
        let (tx, rx) = oneshot::channel();
        let msg = StoreMessage::Snapshot {
            log_path,
            base_dir,
            respond_to: tx,
        };

        self.sender.send(msg)
            .map_err(|_| StoreError::StoreClosed { msg: "actor closed".into() })?;

        rx.await
            .map_err(|_| StoreError::StoreClosed { msg: "response channel closed".into() })?
    }
}

// TCP server
pub async fn run_server(
    address: &str, 
    store_handle: StoreHandle,
    log_path: PathBuf,
    base_dir: PathBuf,
) -> Result<()> {
    let listener = TcpListener::bind(address).await?;
    println!("Server listening on {}", address);

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New client connected: {addr}");
        let handle = store_handle.clone();

        // Clone paths for each connection
        let log_path = log_path.clone();
        let base_dir = base_dir.clone();

        // Spawn a task for each connection
        tokio::spawn(async move{
            if let Err(e) = handle_client(socket, handle, log_path, base_dir).await {
                eprintln!("Error handling client: {e}")
            }
        });
    }
}

async fn handle_client(mut stream: TcpStream, store: StoreHandle, log_path: PathBuf, base_dir: PathBuf) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // EOF
        }

        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        match parts[0] {
            "SET" if parts.len() >= 3 => {
                let key = parts[1].as_bytes().to_vec();
                let value = parts[2..].join(" ").into_bytes();
                match store.set(key, value).await {
                    Ok(()) => writer.write_all(b"OK\n").await?,
                    Err(e) => {
                        writer.write_all(format!("ERROR: {e}\n").as_bytes()).await?;
                    }
                }
            }
            "GET" if parts.len() >=2 => {
                let key = parts[1].as_bytes().to_vec();
                match store.get(key).await {
                    Ok(Some(value)) => {
                        writer.write_all(&value).await?;
                        writer.write_all(b"\n").await?;
                    }
                    Ok(None) => writer.write_all(b"(nil)\n").await?,
                    Err(e) => {
                        writer.write_all(format!("ERROR: {e}\n").as_bytes()).await?;
                    }
                }
            }
            "DEL" if parts.len() >=2 => {
                let key = parts[1].as_bytes().to_vec();
                match store.del(key).await {
                    Ok(true) => writer.write_all(b"1\n").await?,
                    Ok(false) => writer.write_all(b"0\n").await?,
                    Err(e) => {
                        writer.write_all(format!("ERROR: {e}\n").as_bytes()).await?;
                    }
                }
            }
            "SCAN" => {
                let prefix = parts.get(1).map(|s| *s);
                match store.scan(prefix).await {
                    Ok(keys) => {
                        for key in keys {
                            writer.write_all(key.as_bytes()).await?;
                            writer.write_all(b"\n").await?;
                        }
                        writer.write_all(b"OK\n").await?;
                    }
                    Err(e) => {
                        writer.write_all(format!("ERROR: {e}\n").as_bytes()).await?;
                    }
                }
            }
            "SNAPSHOT" => {
                match store.snapshot(log_path.clone(), base_dir.clone()).await {
                    Ok(meta) =>{
                        writer.write_all(
                            format!("OK snapshot-{:04}\n", meta.snapshot_number).as_bytes()
                        ).await?;
                    }
                    Err(e) => {
                        writer.write_all(format!("ERROR: {e}\n").as_bytes()).await?;
                    }
                }
            }
            _ => {
                writer.write_all(b"ERROR: invalid command\n").await?;
            }
            
        }
    }
    Ok(())
}
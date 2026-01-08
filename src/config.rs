#[derive(Debug, Clone)]
pub enum Durability {
    Flush,
    FsyncAlways,
    FsyncEveryN(u64),
}

#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub durability: Durability,
    pub max_log_size: Option<u64>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self { 
            durability: Durability::Flush,
            max_log_size: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotMeta {
    pub snapshot_number: u64,
    pub snapshot_path: std::path::PathBuf,
    pub log_path: std::path::PathBuf,
}
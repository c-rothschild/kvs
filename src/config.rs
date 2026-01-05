#[derive(Debug, Clone)]
pub enum Durability {
    Flush,
    FsyncAlways,
    FsyncEveryN(u64),
}

#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub durability: Durability,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self { durability: Durability::Flush}
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotMeta {
    pub snapshot_number: u64,
    pub snapshot_path: std::path::PathBuf,
    pub log_path: std::path::PathBuf,
}
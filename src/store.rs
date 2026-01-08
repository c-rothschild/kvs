use std::{
    collections::HashMap,
    fs::{OpenOptions, File},
    io::{self, BufReader, BufWriter, Read, Write, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};
use crate::{
    error::{Result, StoreError},
    config::{Durability, StoreOptions, SnapshotMeta},
};


const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;
const MAX_KEY_LEN: usize = 1024;
const MAX_VAL_LEN: usize = 1024 * 1024; // 1 MiB

pub struct Store{
    index: HashMap<Vec<u8>, Arc<Vec<u8>>>,
    log: BufWriter<File>,
    log_path: PathBuf,
    base_dir: PathBuf,
    durability: Durability,
    pending_sync_writes: u64,
    snapshot_number: u64, // Track current snapshot number
    max_log_size: Option<u64>,
    current_log_size: u64,
}

impl Store {
    pub fn open(log_path: impl AsRef<Path>, opts: StoreOptions) -> Result<Self> {
        let log_path = log_path.as_ref().to_path_buf();

        let base_dir = log_path.parent()
            .map(|p| p.canonicalize().unwrap_or_else(|_| p.to_path_buf()))
            .unwrap_or_else(|| {
                std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("."))
            });
        // println!("base dir: {:?}", base_dir);
        let manifest_path = base_dir.join("MANIFEST");

        let manifest = read_manifest(&manifest_path)?;

        let mut index = HashMap::new();
        let actual_log_path: PathBuf;
        let snapshot_number: u64;

        match manifest {
            Some(meta) => {
                actual_log_path = meta.log_path;
                snapshot_number = meta.snapshot_number;

                if meta.snapshot_path.exists() {
                    load_snapshot(&meta.snapshot_path, &mut index)?;
                }
            }
            None => {
                actual_log_path = log_path.clone();
                snapshot_number = 0;
            }
        }
        
        // open once: read+write so replay can truncate;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&actual_log_path)?;

        replay_into(&mut file, &mut index)?; // will truncate if torn tail

        //after replay, go to EOF so appends don't overwrite anything
        file.seek(SeekFrom::End(0))?;
        let current_log_size = file.stream_position()?;
        let log = BufWriter::new(file);

        Ok(Store { 
            index, 
            log,
            log_path: actual_log_path.clone(),
            base_dir,
            durability: opts.durability,
            pending_sync_writes: 0,
            snapshot_number,
            max_log_size: opts.max_log_size,
            current_log_size,
        })

    }
    
    pub fn set(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        validate_kv(key, Some(val))?;
        self.append_set(key, val)?;
        self.index.insert(key.to_vec(), Arc::new(val.to_vec()));
        Ok(())

    }

    pub fn del(&mut self, key: &[u8]) -> Result<bool> {
        validate_kv(key, None)?;
        self.append_del(key)?;
        Ok(self.index.remove(key).is_some())
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.index.get(key).map(|v| v.as_slice())
    }
    pub fn scan_prefix_str(&self, prefix: Option<&str>) -> Vec<String> {
        let mut keys: Vec<String> = self.index
            .keys()
            .filter(|k| {
                match prefix {
                    Some(p) => k.starts_with(p.as_bytes()),
                    None => true
                }
            })
            .filter_map(|k| std::str::from_utf8(k).ok())
            .map(|s| s.to_string())
            .collect();

        keys.sort();
        keys
    }
    fn append_set(&mut self, key: &[u8], val: &[u8]) -> Result<()> {

        self.log.write_all(&[OP_SET])?;
        write_u32(&mut self.log, key.len() as u32)?;
        self.log.write_all(key)?;
        write_u32(&mut self.log, val.len() as u32)?;
        self.log.write_all(val)?;
        self.commit_append()?;
        self.current_log_size += set_record_size(key.len(), val.len());
        self.maybe_auto_snapshot()?;

        Ok(())
    }

    fn append_del(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_all(&[OP_DEL])?;
        write_u32(&mut self.log, key.len() as u32)?;
        self.log.write_all(key)?;
        self.commit_append()?;
        self.current_log_size += del_record_size(key.len());
        self.maybe_auto_snapshot()?;
        Ok(())
    }

    fn commit_append(&mut self) -> Result<()> {
        match self.durability {
            Durability::Flush => {
                self.log.flush()?;
            },
            Durability::FsyncAlways => {
                self.log.flush()?;
                self.log.get_ref().sync_data()?; // OS -> disk (data)
            },
            Durability::FsyncEveryN(n) => {
                self.pending_sync_writes += 1;

                self.log.flush()?;

                if n > 0 && self.pending_sync_writes >= n {
                    self.log.get_ref().sync_data()?;
                    self.pending_sync_writes = 0;
                }
            }
        }
        Ok(())
    }

    pub fn shutdown(mut self) -> Result<()> {
        // Ensure everything makes it out
        self.log.flush()?;
        if !matches!(self.durability, Durability::Flush) {
            self.log.get_ref().sync_data()?;
        }
        Ok(())
    }

    // Renames the old log file and creates a fresh log file at the original path
    pub fn rotate_log(&mut self, log_path: &Path) -> Result<PathBuf> {
        
        use std::time::SystemTime;

        // generate unique name for old log
        let timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut old_log_path = log_path.with_extension(format!("log.{timestamp}"));

        if old_log_path.is_relative() {
            if let Ok(cwd) = std::env::current_dir() {
                old_log_path = cwd.join(&old_log_path);
            }
        }
        

        // flush and close current log
        self.log.flush()?;
        self.log.get_ref().sync_all()?; // sync everything to disk

        // Move curent log to the rotated name
        std::fs::rename(log_path, &old_log_path)?;

        // Open fresh log file
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(log_path)?;

        file.seek(SeekFrom::End(0))?;
        self.log = BufWriter::new(file);

        self.current_log_size = 0;

        Ok(old_log_path)
    }

    pub fn snapshot_view(&self) -> HashMap<Vec<u8>, Arc<Vec<u8>>> {
        // Clone the entire HashMap
        self.index.clone()
    }
    
    fn next_snapshot_number(&mut self) -> u64 {
        self.snapshot_number += 1;
        self.snapshot_number
    }

    pub fn create_snapshot(
        &mut self,
    ) -> Result<SnapshotMeta> {
        // rotate to new log immediately
        let old_log_path = self.rotate_log(&self.log_path.clone())?;

        let view = self.snapshot_view();

        // get next snapshot number
        let snapshot_num = self.next_snapshot_number();
        let snapshot_path = self.base_dir.join(format!("snapshot-{:04}.snap", snapshot_num));

        // write snapshot in current thread
        write_snapshot(view, &snapshot_path)?;

        // write manifest
        let manifest_path = self.base_dir.join("MANIFEST");
        let meta = SnapshotMeta {
            snapshot_number: snapshot_num,
            snapshot_path: snapshot_path.clone(),
            log_path: self.log_path.clone(),
        };
        write_manifest(&manifest_path, &meta)?;

        // clean up old files
        cleanup_old_snapshots(&self.base_dir, snapshot_num)?;

        // delete the rotated log
        if old_log_path.exists() {
            std::fs::remove_file(&old_log_path)?;
        }

        Ok(meta)

    }

    
    fn maybe_auto_snapshot(&mut self) -> Result<()> {
        let Some(max_size) = self.max_log_size else {
            return Ok(());
        };

        if self.current_log_size >= max_size {
            // Trigger snapshot creation
            self.create_snapshot()?;
        }

        Ok(())
    }
    
}

fn write_u32<W: Write>(w: &mut W, n: u32) -> Result<()> {
    w.write_all(&n.to_le_bytes())?;
    Ok(())
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn set_record_size(key_len: usize, val_len: usize) -> u64 {
    1 +
    4 +
    key_len as u64 +
    4 +
    val_len as u64
}

fn del_record_size(key_len: usize) -> u64 {
    1 +
    4 +
    key_len as u64
}


fn replay_into(
    file: &mut File,
    index: &mut HashMap<Vec<u8>, Arc<Vec<u8>>>,
) -> Result<()> {

    let reader_file = file.try_clone()?;
    let mut r = BufReader::new(reader_file);
   
    loop {
        let record_start = r.stream_position()?; // byte offset current record

        //read op byte
        let mut op = [0u8; 1];
        match r.read_exact(&mut op) {
            Ok(_) => {},
            Err(e ) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        // At this point, UnexpectedEof means a torn record -> truncate
        let res: Result<()> = (|| {
            let key_len = read_u32(&mut r)? as usize;
            if key_len == 0 || key_len > MAX_KEY_LEN {
                return Err(StoreError::CorruptLog {
                    msg: format!("invalid key length {key_len} at offset {record_start} during replay")
                });
            }

            let mut key = vec![0u8; key_len];
            r.read_exact(&mut key)?;

            match op[0] {
                OP_SET => {
                    let val_len = read_u32(&mut r)? as usize;
                    if val_len > MAX_VAL_LEN {
                        return Err(StoreError::CorruptLog { 
                            msg: format!("invalid value length {val_len} at offset {record_start} during replay")
                        });
                    }
                    let mut val = vec![0u8; val_len];
                    r.read_exact(&mut val)?;
                    index.insert(key, Arc::new(val));
                    Ok(())
                }
                OP_DEL => {
                    index.remove(&key);
                    Ok(())
                }
                other => {
                    return Err(StoreError::CorruptLog {
                        msg: format!("unknown op code: {other} at offset {record_start}")
                    });
                }
            }
        })();

        match res {
            Ok(()) => continue,
            Err(StoreError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Crash-safe tail handling: truncate torn record
                r.get_ref().set_len(record_start)?;
                break;
            }
            Err(e) => return Err(e),
        }
        
    }

    Ok(())
}

fn validate_kv(key: &[u8], val: Option<&[u8]>) -> Result<()> {
    if key.is_empty() { 
        return Err(StoreError::InvalidInput { msg: "key cannot be empty".into() });
    }
    if key.len() > MAX_KEY_LEN {
        return Err(StoreError::InvalidInput { msg: format!("key too large (>{MAX_KEY_LEN} bytes)") });
    }
    if let Some(v) = val {
        if v.len() > MAX_VAL_LEN {
            return Err(StoreError::InvalidInput { msg: format!("value too large (>{MAX_VAL_LEN} bytes)") });
        }
    }
    Ok(())
}

// Write snapshot view to disk
// called from backgroun thread after getting the view
pub fn write_snapshot(
    view: HashMap<Vec<u8>, Arc<Vec<u8>>>,
    snapshot_path: &Path,
) -> Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = snapshot_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Open temporary file for atomi writing
    let tmp_path = snapshot_path.with_extension("tmp");
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)?;

    let mut writer = BufWriter::new(file);

    // snapshot format is same as log format
    // [key_len: u32][key: bytes][val_len: u32][val: bytes]
    for (key, val_arc) in view.iter() {
        // write key
        write_u32(&mut writer, key.len() as u32)?;
        writer.write_all(key)?;

        //write value
        write_u32(&mut writer, val_arc.len() as u32)?;
        writer.write_all(val_arc.as_slice())?;
    }

    // flush and sync
    writer.flush()?;
    writer.get_ref().sync_all()?;

    // atomically rename temp file to final snapshot
    std::fs::rename(&tmp_path, snapshot_path)
        .map_err(|e| {
            StoreError::Io(io::Error::new(
                e.kind(),
                format!("failed to rename temporary snapshot file {:?} to {:?}: {}", 
                    tmp_path, snapshot_path, e)
            ))
        })?;

    Ok(())


}

// write manifest file that tracks current snapshot/log
pub fn write_manifest(
    manifest_path: &Path,
    snapshot_meta: &SnapshotMeta,
) -> Result<()> {
   let mut file = OpenOptions::new()
    .create(true)
    .truncate(true)
    .write(true)
    .open(manifest_path)?;

    writeln!(
        &mut file,
        "{}:{}:{}",
        snapshot_meta.snapshot_number,
        snapshot_meta.snapshot_path.display(),
        snapshot_meta.log_path.display()

    )?;
    println!("snapshot saved to {}", snapshot_meta.snapshot_path.display());

    file.sync_all()?;

    Ok(())
}

fn cleanup_old_snapshots(base_dir: &Path, current_num: u64) -> Result<()> {
    use std::fs;

    let base_dir = if base_dir == Path::new(".") {
        std::env::current_dir()?
    } else {
        base_dir.canonicalize()
            .or_else(|_| {
                std::env::current_dir()
                    .map(|cwd| cwd.join(base_dir))
                    .and_then(|p| p.canonicalize())
            })
            .unwrap_or_else(|_| base_dir.to_path_buf())
    };

    let entries = fs::read_dir(base_dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("snapshot-") && name.ends_with(".snap") {
                // extract number
                if let Some(num_str) = name.strip_prefix("snapshot-")
                    .and_then(|s| s.strip_suffix(".snap"))
                {
                    if let Ok(num) = num_str.parse::<u64>() {
                        // delete snapshot if older than current number
                        if num < current_num {
                            fs::remove_file(&path)?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn read_manifest(manifest_path: &Path) -> Result<Option<SnapshotMeta>> {
    use std::io::BufRead;
    if !manifest_path.exists() {
        return Ok(None);
    }

    let file = File::open(manifest_path)?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();

    reader.read_line(&mut line)?;
    let line = line.trim();

    let parts: Vec<&str> = line.split(':').collect();
    if parts.len() != 3 {
        return Err(StoreError::CorruptLog { 
            msg: format!("Invalid MANIFEST format: expected 3 colon-seperated parts, got {}", parts.len())
        });
    }

    let snapshot_number: u64 = parts[0].parse()
        .map_err(|e| StoreError::CorruptLog { 
            msg: format!("invalid snapshot number in MANIFEST: {e}")
        })?;

    let snapshot_path = PathBuf::from(parts[1]);
    let log_path = PathBuf::from(parts[2]);

    Ok(Some(SnapshotMeta { snapshot_number, snapshot_path, log_path }))
}

fn load_snapshot(
    snapshot_path: &Path,
    index: &mut HashMap<Vec<u8>, Arc<Vec<u8>>>
) -> Result<()> {
    if !snapshot_path.exists() {
        return Ok(());
    }

    let file = File::open(snapshot_path)?;
    let mut reader = BufReader::new(file);

    // read key-value pairs until EOF
    loop {
        let key_len = match read_u32(&mut reader) {
            Ok(len) => len as usize,
            Err(StoreError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e),
        };

        if key_len == 0 || key_len > MAX_KEY_LEN {
            return Err(StoreError::CorruptLog {
                 msg: format!("Invalid key length {key_len} in snapshot") 
                });
        }

        // Read key
        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        let val_len = read_u32(&mut reader)? as usize;
        if val_len > MAX_VAL_LEN {
            return Err(StoreError::CorruptLog {
                 msg: format!("Invalid value length {val_len} in snapshot") 
                });
        }

        // read val
        let mut val = vec![0u8; val_len];
        reader.read_exact(&mut val)?;

        index.insert(key, Arc::new(val));
    }

    Ok(())
}
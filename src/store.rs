use std::{
    collections::HashMap,
    fs::{OpenOptions, File},
    io::{self, BufReader, BufWriter, Read, Write, Seek, SeekFrom},
    path::{PathBuf, Path},
};
use crate::error::{Result, StoreError};

const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;
const MAX_KEY_LEN: usize = 1024;
const MAX_VAL_LEN: usize = 1024 * 1024; // 1 MiB

pub struct Store{
    index: HashMap<Vec<u8>, Vec<u8>>,
    log_path: PathBuf,
    log: BufWriter<File>,
}

impl Store {
    pub fn open(log_path: impl AsRef<Path>) -> Result<Self> {
        let log_path = log_path.as_ref().to_path_buf();
        
        // open once: read+write so replay can truncate;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&log_path)?;

        let mut index = HashMap::new();

        replay_into(&mut file, &mut index)?; // will truncate if torn tail

        //after replay, go to EOF so appends don't overwrite anything
        file.seek(SeekFrom::End(0))?;
        let log = BufWriter::new(file);

        Ok(Store { index, log_path, log})

    }
    
    pub fn set(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        validate_kv(key, Some(val))?;
        self.append_set(key, val)?;
        self.index.insert(key.to_vec(), val.to_vec());
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

    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = self
            .index
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();

        out.sort();
        out
    }

    pub fn scan_prefix_str(&self, prefix: &str) -> Vec<String> {
        self.scan_prefix(prefix.as_bytes())
            .into_iter()
            .map(|k| String::from_utf8(k).unwrap_or_else(|_| "<non-utf8 key>".to_string()))
            .collect()
    }

    fn append_set(&mut self, key: &[u8], val: &[u8]) -> Result<()> {

        self.log.write_all(&[OP_SET])?;
        write_u32(&mut self.log, key.len() as u32)?;
        self.log.write_all(key)?;
        write_u32(&mut self.log, val.len() as u32)?;
        self.log.write_all(val)?;
        self.log.flush()?; // TODO: add fsync modes

        Ok(())
    }

    fn append_del(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_all(&[OP_DEL])?;
        write_u32(&mut self.log, key.len() as u32)?;
        self.log.write_all(key)?;
        self.log.flush()?;
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


fn replay_into(
    file: &mut File,
    index: &mut HashMap<Vec<u8>, Vec<u8>>,
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
            Err(e) => return Err(StoreError::Io(e)),
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
                    index.insert(key, val);
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
                file.set_len(record_start)?;
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
        return Err(StoreError::InvalidInput { msg: format!("key too large (>{MAX_KEY_LEN} bytes") });
    }
    if let Some(v) = val {
        if v.len() > MAX_VAL_LEN {
            return Err(StoreError::InvalidInput { msg: format!("value too large (>{MAX_VAL_LEN} bytes") });
        }
    }
    Ok(())
}
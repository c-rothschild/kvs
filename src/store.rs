use std::{
    collections::HashMap,
    fs::{OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write, Seek},
    path::PathBuf,
};

const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;

pub struct Store{
    index: HashMap<Vec<u8>, Vec<u8>>,
    log_path: PathBuf,
}

impl Store {
    pub fn open(log_path: impl Into<PathBuf>) -> io::Result<Self> {
        let log_path = log_path.into();
        let mut store = Store { index: HashMap::new(), log_path };
        store.replay()?; //rebuild index from disk
        Ok(store)

    }
    
    pub fn set(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        self.append_set(key, val)?;
        self.index.insert(key.to_vec(), val.to_vec());
        Ok(())

    }

    pub fn del(&mut self, key: &[u8]) -> std::io::Result<bool> {
        self.append_del(key)?;
        Ok(self.index.remove(key).is_some())
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.index.get(key).map(|v| v.as_slice())
    }

    fn append_set(&self, key: &[u8], val: &[u8]) -> io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        let mut w = BufWriter::new(file);

        w.write_all(&[OP_SET])?;
        write_u32(&mut w, key.len() as u32)?;
        w.write_all(key)?;
        write_u32(&mut w, val.len() as u32)?;
        w.write_all(val)?;
        w.flush()?;

        Ok(())
    }

    fn append_del(&self, key: &[u8]) -> io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        let mut w = BufWriter::new(file);

        w.write_all(&[OP_DEL])?;
        write_u32(&mut w, key.len() as u32)?;
        w.write_all(key)?;
        w.flush()?;
        Ok(())
    }


    fn replay(&mut self) -> io::Result<()> {
        let file = match OpenOptions::new().read(true).write(true).open(&self.log_path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };

        let mut r = BufReader::new(file);

        loop {
            let record_start = r.stream_position()?; // byte offset current record

            //read op byte
            let mut op = [0u8; 1];
            match r.read_exact(&mut op) {
                Ok(_) => {},
                Err(e ) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            // At this point, UnexpectedEof means a torn record -> truncate
            let res: io::Result<()> = (|| {
                let key_len = read_u32(&mut r)? as usize;

                let mut key = vec![0u8; key_len];
                r.read_exact(&mut key)?;

                match op[0] {
                    OP_SET => {
                        let val_len = read_u32(&mut r)? as usize;
                        let mut val = vec![0u8; val_len];
                        r.read_exact(&mut val)?;
                        self.index.insert(key, val);
                    }
                    OP_DEL => {
                        self.index.remove(&key);
                    }
                    other => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unknown op code: {other}"),
                        ));
                    }
                }
                Ok(())
            })();

            match res {
                Ok(()) => continue,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Crash-safe tail handling: truncate torn record
                    let f = r.get_ref();            // &File
                    f.set_len(record_start)?;        // drop broken tail
                    break;
                }
                Err(e) => return Err(e)
            }
            
        }

        Ok(())
    }
}

fn write_u32<W: Write>(w: &mut W, n: u32) -> io::Result<()> {
    w.write_all(&n.to_le_bytes())
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}
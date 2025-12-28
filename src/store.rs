use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

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
        self.append_record(key, val)?;
        self.index.insert(key.to_vec(), val.to_vec());
        Ok(())

    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.index.get(key).map(|v| v.as_slice())
    }

    pub fn append_record(&self, key: &[u8], val: &[u8]) -> io::Result<()>{
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        let mut w = BufWriter::new(file);

        write_u32(&mut w, key.len() as u32)?;
        w.write_all(key)?;
        write_u32(&mut w, val.len() as u32)?;
        w.write_all(val)?;
        w.flush()?; // consider fsync for durability
        
        Ok(())
    }

    pub fn replay(&mut self) -> io::Result<()> {
        let file = match File::open(&self.log_path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };

        let mut r = BufReader::new(file);

        loop {
            let key_len = match read_u32(&mut r) {
                Ok(n) => n as usize,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };

            let mut key = vec![0u8; key_len];
            r.read_exact(&mut key)?;

            let val_len = read_u32(&mut r)? as usize;
            let mut val = vec![0u8; val_len];
            r.read_exact(&mut val)?;

            self.index.insert(key, val);
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
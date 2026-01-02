use clap::{Parser, Subcommand};
use kvs::config::{Durability, StoreOptions};
use std::path::PathBuf;

use kvs::store::Store;
use kvs::error::Result;

#[derive(Parser, Debug)]
#[command(name = "kvs", version, about = "Tiny persistent key-value store")]
struct Cli {
    // Path to log file
    #[arg(long, default_value = "data.log")]
    log: PathBuf,

    #[arg(long, default_value = "flush", value_parser = parse_durability)]
    durability: Durability,

    #[command(subcommand)]
    cmd: Command,
}

fn parse_durability(s: &str) -> std::result::Result<Durability, String> {
    match s {
        "flush" => Ok(Durability::Flush),
        "fsync-always" => Ok(Durability::FsyncAlways),
        s if s.starts_with("fsync-every-n:") => {
            let n = s.strip_prefix("fsync-every-n:")
                .unwrap()
                .parse::<u64>()
                .map_err(|e| format!("invalid number: {e}"))?;
            if n == 0 {
                return Err("fsync-every-n value must be greater than 0".to_string());
            }
            Ok(Durability::FsyncEveryN(n))
        }
        _ => Err(format!("invalid durability mode: {s}. Use 'flush', 'fsync-always', or 'fsync-every-n:<number>'"))
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    Set { key: String, value: String},
    Get { key: String },
    Del { key: String },
    Scan { prefix: Option<String> },
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        if e.is_corrupt_log() {
            eprintln!("hint: your data.log appears corrupted (likely a torn write or format mismatch). You can move/delete the log file and try again.");
        }
        // for debugging:
        if std::env::var_os("KVS_DEBUG").is_some() {
            eprintln!("debug: {e:?}");
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    let opts = StoreOptions {
        durability: cli.durability
    };

    let mut store = Store::open(&cli.log, opts)?;

    match cli.cmd {
        Command::Set { key, value } => {
            store.set(key.as_bytes(), value.as_bytes())?;
            println!("OK");

        }
        Command::Del { key } => {
            let existed = store.del(key.as_bytes())?;
            println!("{}", if existed { 1 } else { 0 });
        }
        Command::Get { key } => {
            match store.get(key.as_bytes()) {
                Some(bytes) => {
                    match std::str::from_utf8(bytes) {
                        Ok(s) => println!("{s}"),
                        Err(_) => println!("<non-utf8 value>")
                    }
                }
                None => println!("(nil)"),
            }
        }
        Command::Scan{ prefix } => scan(&store, prefix.as_deref())?,
    }

    store.shutdown()?;
    Ok(())
}

fn scan(store: &Store, prefix: Option<&str>) -> Result<()> {
    let keys = store.scan_prefix_str(prefix);
    
    match prefix {
        Some(p) => {
            if keys.is_empty() {
                println!("0 keys with prefix {:?} found", p);
                return Ok(());
            }
            println!("{} keys with prefix {:?} found:", keys.len(), p);
        }
        None => {
            if keys.is_empty() {
                println!("0 keys found");
                return Ok(());
            }
            println!("{} keys found:", keys.len());
        }
    }
    
    for k in keys {
        println!("  {k}");
    }
    Ok(())
}

use clap::{Parser, Subcommand};
use std::path::PathBuf;

use kvs::store::Store;
use kvs::error::Result;

#[derive(Parser, Debug)]
#[command(name = "kvs", version, about = "Tiny persistent key-value store")]
struct Cli {
    // Path to log file
    #[arg(long, default_value = "data.log")]
    log: PathBuf,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Set { key: String, value: String},
    Get { key: String },
    Del { key: String },
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        if e.is_corrupt_log() {
            eprintln!("hint: your data.log appears corrupted (likely a torn write or format mismatch). You can move/delete the log file and try again.");
        }
        // for debugging:
        eprintln!("debug: {e:?}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    let mut store = Store::open(&cli.log)?;

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
    }

    Ok(())
}

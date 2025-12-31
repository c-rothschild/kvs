mod store;
mod error;

use store::Store;
use crate::error::StoreError;
use std;

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

fn run() -> Result<(), StoreError> {
    Ok(())
}

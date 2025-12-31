use kvs::store::Store;
use kvs::error::StoreError;
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
    let mut store = match Store::open("data.log") {
        Ok(s) => s,
        Err(e) => {
            return Err(e)
        }
    };
    store.set(b"marina", b"hot")?;

    let marina = match store.get(b"marina"){
        Some(e) => String::from_utf8_lossy(e).to_string(),
        None => "None".to_string(),
    };

    println!("Marina is {}", marina);


    Ok(())
}

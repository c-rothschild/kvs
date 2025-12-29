mod store;
use store::Store;
use std;

fn main() -> std::io::Result<()> {
    let mut s = Store::open("data.log")?;
    let a = s.get(b"a");

    if let Some(v) = s.get(b"a") {
        println!("{}", String::from_utf8_lossy(v));
    } else {
        println!("a not found");
    };
    Ok(())
}

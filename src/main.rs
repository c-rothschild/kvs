mod store;
use store::Store;
use std;

fn main() -> std::io::Result<()> {
    let mut s = Store::open("data.log")?;
    s.set(b"score", b"12")?;
    println!("{:?}", std::str::from_utf8(s.get(b"score").unwrap()).unwrap());
    Ok(())
}

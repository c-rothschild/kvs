mod store;
use store::Store;
use std;

fn main() -> std::io::Result<()> {
    let mut s = Store::open("data.log")?;
    s.set(b"a", b"1")?;
    s.set(b"a", b"2")?;
    s.del(b"a")?;
    assert!(s.get(b"a").is_none());
    Ok(())
}

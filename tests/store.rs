use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use kvs::store::Store;

fn fresh_log_path(test_name: &str) -> PathBuf {
    // Create unique(ish) file per test run
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!("kvs_{test_name}_{pid}_{nanos}.log"));
    p
}
#[test]
fn set_get_roundtrip() {
    let path = fresh_log_path("set_get_roundtrip");
    let mut s = Store::open(&path).unwrap();

    s.set(b"score", b"12").unwrap();
    assert_eq!(s.get(b"score").unwrap(), b"12");

    drop(s);
    let _ = fs::remove_file(path);
}

#[test]
fn overwrite_last_write_wins() {
    let path = fresh_log_path("overwrite_last_write_wins");
    let mut s = Store::open(&path).unwrap();

    s.set(b"k", b"v1").unwrap();
    s.set(b"k", b"v2").unwrap();
    assert_eq!(s.get(b"k").unwrap(), b"v2");

    drop(s);
    let _ = fs::remove_file(path);
}

#[test]
fn del_removes_key() {
    let path = fresh_log_path("del_removes_key");
    let mut s = Store::open(&path).unwrap();

    s.set(b"a", b"1").unwrap();
    let existed = s.del(b"a").unwrap();
    assert!(existed);
    assert!(s.get(b"a").is_none());

    drop(s);
    let _ = fs::remove_file(path);
}

#[test]
fn del_missing_returns_false_and_keeps_missing() {
    let path = fresh_log_path("del_missing_returns_false_and_keeps_missing");
    let mut s = Store::open(&path).unwrap();

    let existed = s.del(b"nope").unwrap();
    assert!(!existed);
    assert!(s.get(b"nope").is_none());

    drop(s);
    let _ = fs::remove_file(path);
}

#[test]
fn reopen_replays_state() {
    let path = fresh_log_path("reopen_replays_state");
    
    {
        let mut s = Store::open(&path).unwrap();
        s.set(b"a", b"1").unwrap();
        s.set(b"b", b"2").unwrap();
        s.del(b"a").unwrap();
    }

    {
        let s = Store::open(&path).unwrap();
        assert!(s.get(b"a").is_none());
        assert_eq!(s.get(b"b").unwrap(), b"2");
    }

    let _ = fs::remove_file(path);
}

#[test]
fn torn_tail_is_truncated_and_does_not_lose_prior_records() {
    let path = fresh_log_path("torn_tail");

    // Write some records 
    {
        let mut s = Store::open(&path).unwrap();
        s.set(b"a", b"1").unwrap();
        s.set(b"b", b"2").unwrap();
        s.set(b"c", b"3").unwrap();
    }

    // Simulate crash mid-write: chop off last few bytes of file
    let original_len = fs::metadata(&path).unwrap().len();

    {
        let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        // remove 3 bytes
        let new_len = original_len.saturating_sub(3);
        f.set_len(new_len).unwrap();
        f.flush().unwrap();
    }

    // Reopen should truncate broken tail and keep earlier keys
    {
        let s = Store::open(&path).unwrap();
        assert_eq!(s.get(b"a").unwrap(), b"1");
        assert_eq!(s.get(b"b").unwrap(), b"2");
        // "c" might be missing depending on where the cut landed; the point is:
        // - open() doesn't crash
        // - earlier records are intact
    }

    let truncated_len = fs::metadata(&path).unwrap().len();
    assert!(truncated_len <= original_len);

    let _ = fs::remove_file(path);

}

#[test]
fn scan_prefix_returns_sorted_matches() {
    let path = fresh_log_path("scan_prefix");
    let mut s = Store::open(&path).unwrap();

    s.set(b"app", b"1").unwrap();
    s.set(b"apple", b"2").unwrap();
    s.set(b"banana", b"3").unwrap();
    s.set(b"apricot", b"4").unwrap();

    let filtered_keys = s.scan_prefix_str(Some("ap"));
    assert_eq!(filtered_keys, ["app", "apple", "apricot"]);

    let all_keys = s.scan_prefix_str(None);
    assert_eq!(all_keys, ["app", "apple", "apricot", "banana"]);

    let _ = fs::remove_file(path);

}

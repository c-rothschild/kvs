use assert_cmd::cargo::cargo_bin_cmd;
use predicates::str::contains;
use tempfile::NamedTempFile;

#[test]
fn cli_set_then_get_persists() {
    let log = NamedTempFile::new().unwrap();
    let log_path = log.path().to_str().unwrap();

    // kvs --log <path> set a 1
    cargo_bin_cmd!("kvs")
        .args(["--log", log_path, "set", "a", "1"])
        .assert()
        .success();

    // kvs --log <path> get a => prints 1
    cargo_bin_cmd!("kvs")
        .args(["--log", log_path, "get", "a"])
        .assert()
        .success()
        .stdout(contains("1"));
}

#[test]
fn cli_del_returns_1_then_0() {
    let log = NamedTempFile::new().unwrap();
    let log_path = log.path().to_str().unwrap();

    cargo_bin_cmd!("kvs")
        .args(["--log", log_path, "set", "x", "y"])
        .assert()
        .success();

    // delete x - should return 1
    cargo_bin_cmd!("kvs")
        .args(["--log", log_path, "del", "x"])
        .assert()
        .success()
        .stdout(contains("1"));

    cargo_bin_cmd!("kvs")
        .args(["--log", log_path, "del", "x"])
        .assert()
        .success()
        .stdout(contains("0"));
}

#[test]
fn cli_get_missing_prints_nil() {
    let log = NamedTempFile::new().unwrap();
    let log_path = log.path().to_str().unwrap();

    cargo_bin_cmd!("kvs")
        .args(["--log", log_path, "get", "nothing"])
        .assert()
        .success()
        .stdout(contains("(nil)"));
}
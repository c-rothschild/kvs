# KVS - Key-Value Store

A lightweight, persistent key-value store written in Rust with both a command-line interface and TCP server support.

## Features

- **Persistent Storage**: Data is written to a log file (`data.log` by default) and survives restarts
- **Crash-Safe**: Handles torn writes and corrupted log tails gracefully
- **Thread-Safe TCP Server**: Actor model implementation with a single store thread for safe concurrent access
- **Durability Modes**: Configurable durability levels (flush, fsync-always, fsync-every-n)
- **Prefix Scanning**: Query keys by prefix or list all keys
- **Tombstone Deletion**: Proper handling of deleted keys
- **Manual Snapshots**: Create point-in-time snapshots to reduce log file size
- **Auto-Snapshots**: Automatically create snapshots when log file reaches a configurable size threshold
- **Efficient Size Tracking**: Manual file size tracking for fast snapshot triggering without OS overhead
- **Error Handling**: Comprehensive error types for I/O, corruption, and invalid input

## Installation

Build from source:

```bash
cargo build --release
```

## Command-Line Interface

### Basic Usage

The CLI supports five operations: `set`, `get`, `del`, `scan`, and `snapshot`.

#### Set a Key-Value Pair

```bash
cargo run -- set mykey "my value"
```

Output: `OK`

#### Get a Value

```bash
cargo run -- get mykey
```

Output: `my value` (or `(nil)` if not found)

#### Delete a Key

```bash
cargo run -- del mykey
```

Output: `1` (deleted) or `0` (not found)

#### Scan Keys

List all keys:
```bash
cargo run -- scan
```

List keys with a prefix:
```bash
cargo run -- scan user:
```

#### Create a Snapshot

Manually create a snapshot to compress the log file:
```bash
cargo run -- snapshot
```

Output: `snapshot saved to snapshot-0001.snap` (or similar)

Snapshots create a compressed point-in-time copy of your data and reset the log file, which helps reduce log file size over time.

### Options

**Custom Log File Path**

```bash
cargo run -- --log /path/to/custom.log set key value
```

**Durability Modes**

The `--durability` flag controls how data is persisted to disk:

- `flush` (default): Flushes to OS buffers only
  ```bash
  cargo run -- --durability flush set key value
  ```

- `fsync-always`: Forces data to disk on every write (most durable, slowest)
  ```bash
  cargo run -- --durability fsync-always set key value
  ```

- `fsync-every-n:<number>`: Syncs to disk every N writes (balanced performance/durability)
  ```bash
  cargo run -- --durability fsync-every-n:10 set key value
  ```

**Auto-Snapshot (Maximum Log Size)**

Automatically create snapshots when the log file reaches a specified size. This helps prevent log files from growing indefinitely.

Size can be specified with units (KB, MB, GB) or as raw bytes:

```bash
# Auto-snapshot at 10 MB
cargo run -- --max-log-size 10MB set key value

# Auto-snapshot at 100 MB
cargo run -- --max-log-size 100MB server

# Auto-snapshot at 1 GB
cargo run -- --max-log-size 1GB server

# Auto-snapshot at specific byte count (10485760 = 10 MB)
cargo run -- --max-log-size 10485760 server
```

When enabled, the store will automatically create a snapshot and rotate the log file whenever the log reaches or exceeds the specified size. This feature uses manual file size tracking for optimal performance, avoiding OS metadata queries on every write.

**Combining Options**

You can combine multiple options:
```bash
cargo run -- --log /custom/path.log --durability fsync-always --max-log-size 50MB server --addr 0.0.0.0:9000
```

## Snapshots

KVS supports both manual and automatic snapshots to help manage log file growth and improve recovery performance.

### How Snapshots Work

1. **Snapshot Creation**: Creates a point-in-time copy of all key-value pairs in a compact binary format
2. **Log Rotation**: The current log file is rotated (renamed) and a fresh log file is created
3. **Log Deletion**: After successful snapshot creation, the old log file is deleted
4. **Recovery**: On startup, KVS first loads the snapshot (fast), then replays any log entries written after the snapshot

### Manual Snapshots

Create snapshots on-demand using the CLI:

```bash
# Create a snapshot manually
cargo run -- snapshot

# With custom log path
cargo run -- --log /path/to/data.log snapshot
```

Snapshots are stored in the same directory as the log file with names like `snapshot-0001.snap`, `snapshot-0002.snap`, etc. The MANIFEST file tracks which snapshot and log file are current.

### Automatic Snapshots

Enable automatic snapshots by specifying `--max-log-size` when starting the server or running CLI commands:

```bash
# Server with auto-snapshot at 10 MB
cargo run -- --max-log-size 10MB server

# CLI operations with auto-snapshot at 100 MB
cargo run -- --max-log-size 100MB set key value
```

**Performance**: Auto-snapshot uses manual file size tracking (counting bytes written) rather than querying the OS file system. This provides:
- **Fast Size Checks**: No OS metadata queries on every write operation
- **Accurate Tracking**: Precisely tracks the size of data written to the log
- **Low Overhead**: Minimal performance impact on write operations

### Snapshot Management

- Only the most recent snapshot is kept (older snapshots are automatically deleted)
- Snapshots are numbered sequentially (`snapshot-0001.snap`, `snapshot-0002.snap`, etc.)
- The `MANIFEST` file in the data directory tracks the current snapshot and log file

## TCP Server

The TCP server allows multiple clients to connect and perform operations concurrently using the Actor model pattern for thread safety.

### Starting the Server

```bash
cargo run -- server
```

This starts the server on `127.0.0.1:8080` by default.

To specify a custom address:
```bash
cargo run -- server --addr 0.0.0.0:9000
```

To start with auto-snapshot enabled:
```bash
# Auto-snapshot at 10 MB
cargo run -- --max-log-size 10MB server

# Custom address with auto-snapshot
cargo run -- --max-log-size 50MB server --addr 0.0.0.0:9000
```

The server will print:
```
Server listening on 127.0.0.1:8080
```

When a snapshot is automatically created, you'll see:
```
snapshot saved to snapshot-0001.snap
```

### Protocol

The server uses a simple line-based protocol. Each command ends with a newline (`\n`).

#### Commands

**SET**: Store a key-value pair
```
SET <key> <value>
```

Response: `OK\n` or `ERROR: <message>\n`

**GET**: Retrieve a value
```
GET <key>
```

Response: `<value>\n`, `(nil)\n` (not found), or `ERROR: <message>\n`

**DEL**: Delete a key
```
DEL <key>
```

Response: `1\n` (deleted), `0\n` (not found), or `ERROR: <message>\n`

**SCAN**: List keys (optionally filtered by prefix)
```
SCAN [prefix]
```

Response: One key per line, followed by `OK\n`, or `ERROR: <message>\n`

**SNAPSHOT**: Manually trigger a snapshot creation
```
SNAPSHOT
```

Response: `OK snapshot-0001\n` (with snapshot number) or `ERROR: <message>\n`

### Testing the Server

#### Using `nc` (netcat)

Connect interactively:
```bash
nc 127.0.0.1 8080
```

Then type commands:
```
SET name Alice
GET name
SET age 30
SCAN
DEL name
GET name
```

Press `Ctrl+C` or `Ctrl+D` to disconnect.

#### Using `echo` for Quick Tests

```bash
echo "SET test value" | nc 127.0.0.1 8080
echo "GET test" | nc 127.0.0.1 8080
echo "DEL test" | nc 127.0.0.1 8080
```

#### Example Session

```bash
$ nc 127.0.0.1 8080
SET user:alice Alice Smith
OK
SET user:bob Bob Jones
OK
GET user:alice
Alice Smith
SCAN user:
user:alice
user:bob
OK
SNAPSHOT
OK snapshot-0001
DEL user:alice
1
GET user:alice
(nil)
```

### Server Architecture

The server uses the **Actor Model** pattern:

- **Single Store Thread**: One dedicated thread owns the `Store` and processes all operations sequentially
- **Message Passing**: TCP handler tasks send messages through channels to the store actor
- **Concurrent Clients**: Multiple clients can connect simultaneously; each gets its own async task
- **Thread Safety**: No locks needed - the actor pattern ensures all store operations happen in one thread

This design provides:
- Thread-safe concurrent access without mutexes
- Sequential operation processing (easier to reason about)
- High performance for concurrent reads/writes

## Error Handling

The store provides detailed error messages:

- **I/O Errors**: File system issues (permissions, disk full, etc.)
- **Corrupt Log**: Invalid log format or torn writes
  - Tip: If you see this error, you can delete or move the log file to start fresh
- **Invalid Input**: Empty keys, oversized keys/values (max 1KB key, 1MB value)

Enable debug mode for detailed error information:
```bash
KVS_DEBUG=1 cargo run -- get key
```

## File Format

### Log File

Data is stored in `data.log` using a simple binary format:
- Operations (SET/DEL) are appended sequentially
- On startup, the log is replayed to rebuild the in-memory index
- Torn writes at the end of the log are automatically truncated during recovery

### Snapshot Files

Snapshots are stored as `snapshot-NNNN.snap` files:
- Compact binary format containing all key-value pairs
- Same format as the log (key-length, key, value-length, value)
- Used for faster recovery: load snapshot first, then replay log

### MANIFEST File

The `MANIFEST` file tracks the current state:
- Format: `<snapshot_number>:<snapshot_path>:<log_path>`
- Updated whenever a new snapshot is created
- Used during startup to locate the current snapshot and log files

## Limitations

- Maximum key length: 1 KB (1024 bytes)
- Maximum value length: 1 MB (1,048,576 bytes)
- Keys and values are stored as binary data (UTF-8 for text)

## Development

Run tests:
```bash
cargo test
```

Run with debug output:
```bash
KVS_DEBUG=1 cargo run -- get key
```

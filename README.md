# Cassandra-like Distributed Database

A distributed key-value store implementation inspired by Apache Cassandra, featuring consistent hashing, replication, fault tolerance, and crash recovery.

## Features

### Distributed Architecture
- **Consistent Hashing Ring**: Keys are distributed across nodes using SHA-1 hashing
- **Replication**: Configurable replication factor (default: 2) with automatic replica placement
- **Node Management**: Dynamic node addition/removal with automatic ring updates

### Durability & Crash Recovery
- **In-Memory Storage**: Fast dictionary-based key-value storage with timestamp tracking
- **Commit Log**: All mutations (PUT/DELETE/REPLICATE) are immediately written to disk for crash recovery
- **Periodic Snapshots**: Full state snapshots written to disk at configurable intervals (default: 500ms)
- **Automatic Recovery**: Nodes recover from disk (snapshot + commit log replay) on startup
- **Timestamp-Based Conflict Resolution**: Last-write-wins using timestamps for concurrent operations

### Fault Tolerance
- **Network Fault Simulation**: Configurable delays, drops, duplicates, and partitions
- **Node Failure Handling**: Nodes can fail and recover with state restoration
- **Replication-Based Resilience**: Data survives individual node failures
- **Coordinator Forwarding**: Writes to failed primary nodes are automatically forwarded to alive replicas

### Deterministic Simulation
- **Simulated Network**: Event-driven network simulator for testing without real network overhead
- **Deterministic Clock**: Predictable time advancement for reproducible results
- **Network Statistics**: Track dropped, delivered, and duplicated messages

## Project Structure

```
cassandra-project/
├── core/
│   ├── node.py          # Node implementation with storage, replication, recovery
│   ├── ring.py          # Consistent hashing ring for key distribution
│   ├── gossip.py        # Gossip protocol for cluster membership
│   └── storage.py       # Storage manager with commit logs and snapshots
├── sim/
│   ├── network.py       # Network simulator with fault injection
│   └── scheduler.py     # Deterministic event scheduler
├── dashboard/
│   ├── app.py           # Streamlit visualization dashboard
│   └── README.md        # Dashboard documentation
├── logs/                # Per-node commit logs and snapshots
│   └── {node_id}/
│       ├── commit.log   # Append-only mutation log (PUT/DELETE)
│       └── snapshot.json # Periodic full state snapshot with timestamps
├── main.py              # Demo simulation
├── requirements.txt     # Python dependencies
├── run_dashboard.sh     # Convenience script to run dashboard
└── README.md
```

## Installation & Usage

### Quick Start

```bash
# Clone the repository
git clone <repository-url>
cd cassandra-project

# Install dependencies
pip install -r requirements.txt

# Run the demo simulation
python3 main.py
```

### Visualization Dashboard

The project includes an interactive Streamlit dashboard to visualize the system in real-time:

```bash
# Run the visualization dashboard
streamlit run dashboard/app.py

# Or use the convenience script
./run_dashboard.sh
```

See `dashboard/README.md` for more details.


### Testing
```bash
python -m testing.test_gossip
python -m testing.test_replication
python -m testing.test_crash_recovery
python -m testing.test_partition_tolerance

python main.py # to see the process
```

## Architecture

### Consistent Hashing Ring

Keys are mapped to nodes using SHA-1 hashing:
- Each node has a unique hash position on the ring
- Each key hashes to a position on the ring
- The node responsible for a key is the first node clockwise from the key's hash

### Replication

- Replication Factor (RF): Number of replicas per key (default: 2)
- Replica placement: Primary node + next RF-1 nodes clockwise on ring
- Writes are replicated asynchronously to all replica nodes

### Write Path

1. Client calls `node.put(key, value)`
2. System determines primary node using consistent hashing
3. If primary is down, request is forwarded to first alive replica as coordinator
4. Write applied to primary's in-memory store with timestamp
5. Write appended to commit log (disk) with timestamp and mutation type
6. Write replicated to RF-1 other nodes with timestamp
7. Periodic snapshots capture full state including timestamps

### Delete Path

1. Client calls `node.delete(key)`
2. System determines primary node using consistent hashing
3. Deletion marker (tombstone) stored in primary's in-memory store with timestamp
4. Deletion appended to commit log (disk) with timestamp and DELETE type
5. Deletion replicated to RF-1 other nodes with timestamp

### Read Path

1. Client calls `node.get(key)`
2. System determines primary node
3. Value returned from primary's in-memory store
4. If key is marked as deleted (tombstone), returns None
5. If primary is down, read from replica

### Crash Recovery

On node startup/recovery:
1. Load latest snapshot from disk (if exists) restores state with timestamps
2. Replay commit log entries after snapshot applies PUT and DELETE operations with timestamps
3. Request missing state from peer nodes via snapshot request
4. Merge received snapshot using timestamp-based conflict resolution (newer timestamps win)
5. Resume normal operation


### Node Failure & Recovery

```python
# Simulate node failure
node2.fail()

# Node ignores all incoming messages while failed
node1.put("key", "value")  # Replicated to other nodes

# Recover node (loads from disk, requests missing state)
node2.recover()
```

## Disk Storage

Each node maintains its own storage directory under `logs/{node_id}/`:

- **`commit.log`**: Append-only log of all mutations (JSON lines with timestamps)
  ```
  {"key": "apple", "value": "red", "timestamp": 1234567890.123, "type": "PUT"}
  {"key": "banana", "value": "yellow", "timestamp": 1234567890.456, "type": "PUT"}
  {"key": "apple", "value": {"deleted": true}, "timestamp": 1234567890.789, "type": "DELETE"}
  ```

- **`snapshot.json`**: Periodic full state snapshot (JSON object with timestamps)
  ```json
  {
    "apple": {"value": "red", "timestamp": 1234567890.123},
    "banana": {"value": "yellow", "timestamp": 1234567890.456}
  }
  ```
  Note: Deleted keys are stored as `{"deleted": true, "timestamp": ...}`

Recovery process:
1. Load `snapshot.json` to restore base state with timestamps
2. Replay entries from `commit.log` written after snapshot (respecting timestamps)
3. Request snapshot from peer nodes
4. Merge peer snapshots using timestamp-based conflict resolution

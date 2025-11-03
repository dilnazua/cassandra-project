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
│   └── ring.py          # Consistent hashing ring for key distribution
├── sim/
│   ├── network.py       # Network simulator with fault injection
│   └── scheduler.py     # Deterministic event scheduler
├── logs/                # Per-node commit logs and snapshots
│   └── {node_id}/
│       ├── commit.log   # Append-only mutation log (PUT/DELETE)
│       └── snapshot.json # Periodic full state snapshot with timestamps
├── main.py              # Demo simulation
├── requirements.txt     # Python dependencies
└── README.md
```

## Installation & Usage

### Quick Start

```bash
# Clone the repository
git clone <repository-url>
cd cassandra-project

# Run the demo simulation
python3 main.py
```

### Example Output

```
[Node C] Stored key=apple, value=red
[Node C] Stored key=banana, value=yellow
[Node B] FAIL
[Node B] RECOVER -> loading from disk
[Node B] Snapshot written: 2 keys
[Node C] Snapshot written: 2 keys

Final states:
Node A: {}
Node B: {'apple': 'red', 'banana': 'yellow'}
Node C: {'banana': 'yellow', 'apple': 'red'}
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
1. Load latest snapshot from disk (if exists) - restores state with timestamps
2. Replay commit log entries after snapshot - applies PUT and DELETE operations with timestamps
3. Request missing state from peer nodes via snapshot request
4. Merge received snapshot using timestamp-based conflict resolution (newer timestamps win)
5. Resume normal operation

## Usage Examples

### Basic Operations

```python
from sim.scheduler import SimClock, SimScheduler
from sim.network import SimNetwork
from core.ring import Ring
from core.node import Node

# Setup
clock = SimClock()
scheduler = SimScheduler(clock)
network = SimNetwork(scheduler)
ring = Ring()

# Create nodes (manual IDs for debugging)
node1 = Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False)
node2 = Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False)
node3 = Node("C", network, scheduler, ring, replication_factor=2, use_uuid=False)

# Register nodes
for node in [node1, node2, node3]:
    ring.add_node(node.node_id, node)

# Perform operations
node1.put("key1", "value1")
node2.put("key2", "value2")
result = node3.get("key1")
node1.delete("key1")  # Delete a key

# Advance simulation
scheduler.run_due()
clock.advance(100)
```

### Using Auto-Generated UUIDs

```python
# Auto-generate UUID node IDs (more realistic)
node1 = Node(network=network, scheduler=scheduler, ring=ring)
node2 = Node(network=network, scheduler=scheduler, ring=ring)
node3 = Node(network=network, scheduler=scheduler, ring=ring)

print(node1.node_id)  # e.g., "a3f5b2c1-8d9e-4f1a-b2c3-d4e5f6a7b8c9"
```

### Network Fault Simulation

```python
from sim.network import drop, delay, duplicate, partition

# Add network faults
network.add_rule(delay(50, 150))      # Random delays 50-150ms
network.add_rule(drop(0.05))          # Drop 5% of messages
network.add_rule(duplicate(0.05))     # Duplicate 5% of messages

# Network partition (block traffic between specific nodes)
network.add_rule(partition({("A", "B")}))
```

### Delete Operations

```python
# Delete a key
node1.delete("key1")

# Deleted keys return None
result = node1.get("key1")  # Returns None

# Deletions are replicated to all replicas
# Deletions are logged to commit log as tombstones
```

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

## Configuration

### Node Parameters

- `node_id`: Optional manual ID (default: auto-generate UUID)
- `replication_factor`: Number of replicas per key (default: 2)
- `snapshot_interval_ms`: Time between snapshots in milliseconds (default: 500)
- `log_dir`: Directory for commit logs and snapshots (default: "logs")
- `use_uuid`: Whether to auto-generate UUID even if node_id provided (default: True)

### Network Simulation

- `seed`: Random seed for reproducible network delays (default: 0)
- Fault injection rules: `drop(p)`, `delay(min_ms, max_ms)`, `duplicate(p)`, `partition(cut)`

## Testing

The system is designed for deterministic testing:

```python
# Same seed = same behavior
network = SimNetwork(scheduler, seed=42)

# Run simulation in steps
for _ in range(10):
    scheduler.run_due()      # Process all due events
    clock.advance(50)        # Advance time by 50ms
```

## Limitations & Future Work

Current limitations:
- Simple conflict resolution (last-write-wins based on timestamps, no vector clocks)
- No hinted handoff for writes during node failures
- Simple recovery (requests snapshot from one successor only)
- No compaction of commit logs (tombstones accumulate indefinitely)
- In-memory only (no persistence beyond commit log + snapshots)
- Deleted keys remain as tombstones in memory (no expiration)

Potential enhancements:
- Vector clocks for more sophisticated conflict resolution
- Anti-entropy repair mechanisms (Merkle trees)
- Gossip protocol for cluster membership
- Read repair for stale data
- Hinted handoff queue for writes during node failures
- Commit log compaction (remove old tombstones)
- Tombstone expiration (remove old deletion markers)
- Multi-version storage with retention policies

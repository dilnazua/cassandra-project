import logging
import json
import os
import uuid
from pathlib import Path
import time

class Node:
    """Represents a database node in the distributed ring."""

    def __init__(self, node_id=None, network=None, scheduler=None, ring=None, 
                 replication_factor: int = 2, snapshot_interval_ms: int = 500, 
                 log_dir: str = "logs", use_uuid: bool = True, grace_period_seconds: int = 864000):
        # Auto-generate UUID if no node_id provided
        if node_id is None:
            self.node_id = str(uuid.uuid4())
        elif use_uuid:
            self.node_id = str(uuid.uuid4())
        else:
            # Use provided ID for testing
            self.node_id = node_id
        self.network = network
        self.scheduler = scheduler
        self.ring = ring
        self.data = {}  # key-value store (in-memory)
        self.alive = True
        self.replication_factor = replication_factor
        self.snapshot_interval_ms = snapshot_interval_ms
        self.grace_period_seconds = grace_period_seconds
        
        # Setup disk storage paths
        self.log_dir = Path(log_dir) / self.node_id
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.commit_log_path = self.log_dir / "commit.log"
        self.snapshot_path = self.log_dir / "snapshot.json"
        
        if not self.commit_log_path.exists():
            self.commit_log_path.touch()
        
        # Recover from disk on startup
        self._recover_from_disk()
        
        # Schedule periodic snapshots
        self._schedule_snapshot()

        if network:
            network.register(self.node_id, self.on_receive)

    def put(self, key, value):
        """
        Client API: store key in the correct node.
        
        Note: In consistent hashing, keys are routed to their primary node based on hash.
        Only the primary node and its replicas store the data. If the calling node is not
        the primary, it forwards the request to the primary node.
        """
        target = self.ring.get_node(key)
        if target == self:
            logging.info(f"[Node {self.node_id}] PUT {key}={value} -> storing locally (I am primary)")
            # Store first to get the timestamp
            timestamp = time.time()
            self._local_put(key, value, timestamp=timestamp)
            # Trigger replication from primary node with timestamp
            self._replicate(key, value, timestamp=timestamp)
        else:
            # Check if primary is alive, if not forward to first alive replica as coordinator
            if not target.alive:
                replicas = self.ring.get_replicas(key, self.replication_factor)
                # Try to find first alive replica to act as coordinator
                coordinator = None
                for replica in replicas:
                    if replica.alive and replica.node_id != target.node_id:
                        coordinator = replica
                        break
                
                if coordinator:
                    logging.info(f"[Node {self.node_id}] PUT {key}={value} -> primary {target.node_id} is down, forwarding to replica coordinator {coordinator.node_id}")
                    self.network.send(coordinator.node_id, {
                        "type": "PUT",
                        "key": key,
                        "value": value,
                        "from": self.node_id,
                    })
                else:
                    logging.error(f"[Node {self.node_id}] PUT {key}={value} -> FAILED: primary {target.node_id} is down and no alive replicas")
            else:
                logging.info(f"[Node {self.node_id}] PUT {key}={value} -> forwarding to primary Node {target.node_id}")
                self.network.send(target.node_id, {
                    "type": "PUT",
                    "key": key,
                    "value": value,
                    "from": self.node_id,
                })

    def get(self, key):
        """Client API: retrieve key from correct node."""
        target = self.ring.get_node(key)
        if target == self:
            return self._local_get(key)
        else:
            self.network.send(target.node_id, {
                "type": "GET",
                "key": key,
                "from": self.node_id,
            })
    

    def delete(self, key):
        """Client API: delete key from the system."""
        target = self.ring.get_node(key)
        if target == self:
            timestamp = time.time()
            self._local_delete(key, timestamp=timestamp)
            # Replicate the delete to replicas
            self._replicate_delete(key, timestamp=timestamp)
        else:
            if not target.alive:
                replicas = self.ring.get_replicas(key, self.replication_factor)
                # Try to find first alive replica to act as coordinator
                coordinator = None
                for replica in replicas:
                    if replica.alive and replica.node_id != target.node_id:
                        coordinator = replica
                        break
                if coordinator:
                    logging.info(f"[Node {self.node_id}] DELETE {key} -> primary {target.node_id} is down, forwarding to replica coordinator {coordinator.node_id}")
                    self.network.send(coordinator.node_id, {
                        "type": "DELETE",
                        "key": key,
                        "from": self.node_id
                    })
                else:
                    logging.error(f"[Node {self.node_id}] DELETE {key} -> FAILED: primary {target.node_id} is down and no alive replicas")
            else:
                logging.info(f"[Node {self.node_id}] DELETE {key} -> forwarding to primary Node {target.node_id}")
                self.network.send(target.node_id, {
                    "type": "DELETE",
                    "key": key,
                    "from": self.node_id
            })


    # Internal methods
    def _local_put(self, key, value, timestamp=None):
        """Store key-value pair : {"value": value, "timestamp": timestamp}"""
        current = self.data.get(key)
        current_ts = current.get("timestamp", 0) if isinstance(current, dict) else 0
        
        if timestamp is None:
            timestamp = time.time()
        
        if timestamp > current_ts:
            self.data[key] = {"value": value, "timestamp": timestamp}
            # Store just the value string in commit log, not the full dict
            self._append_commit_log(key, value, mutation_type="PUT", timestamp=timestamp)
            logging.info(f"[Node {self.node_id}] Stored key={key}, value={value}, ts={timestamp}")
        elif timestamp == current_ts:
            # Idempotent duplicate - don't update or log
            logging.info(f"[Node {self.node_id}] Duplicate for key={key}, value={value}, ts={timestamp} - skipping")

    
    def _append_commit_log(self, key, value, mutation_type="PUT", timestamp=None):
        if timestamp is None:
            timestamp = time.time()
        entry = {
            "key": key,
            "value": value,
            "timestamp": timestamp,
            "type": mutation_type
        }
        try:
            with open(self.commit_log_path, 'a') as f:
                f.write(json.dumps(entry) + "\n")
                f.flush()
        except Exception as e:
            logging.error(f"[Node {self.node_id}] Failed to write commit log: {e}")


    
    def _write_snapshot(self):
        """Write current in-memory state to disk snapshot.
            In Cassandra, the snapshot is discarded after it is written, but for a demo, we keep it.
            Compacts expired tombstones before writing snapshot.
        """
        # Compact expired tombstones before snapshot (like Cassandra compaction)
        self._compact_tombstones()
        
        try:
            # Write to temporary file first, then rename
            temp_path = self.snapshot_path.with_suffix('.tmp')
            with open(temp_path, 'w') as f:
                json.dump(self.data, f, indent=2)
            temp_path.replace(self.snapshot_path)
            logging.info(f"[Node {self.node_id}] Snapshot written: {len(self.data)} keys")
        except Exception as e:
            logging.error(f"[Node {self.node_id}] Failed to write snapshot: {e}")
    
    def _schedule_snapshot(self):
        """Schedule periodic snapshot writes."""
        if self.alive and self.scheduler:
            self.scheduler.call_later(self.snapshot_interval_ms, self._take_snapshot)
    
    def _take_snapshot(self):
        """Take a snapshot and schedule the next one."""
        if self.alive:
            self._write_snapshot()
            self._schedule_snapshot()
    
    def _recover_from_disk(self):
        """Recover node state from disk: load snapshot then replay commit log."""
        # Load snapshot if it exists 
        if self.snapshot_path.exists():
            try:
                with open(self.snapshot_path, 'r') as f:
                    self.data = json.load(f)
                logging.info(f"[Node {self.node_id}] Loaded snapshot: {len(self.data)} keys")
            except Exception as e:
                logging.warning(f"[Node {self.node_id}] Failed to load snapshot: {e}")
                self.data = {}
        
        # Replay commit log entries after snapshot
        if self.commit_log_path.exists():
            try:
                with open(self.commit_log_path, 'r') as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line)
                            timestamp = entry.get("timestamp", time.time())
                            if entry.get("type") == "DELETE":
                                self.data[entry["key"]] = {"deleted": True, "timestamp": timestamp}
                            else:
                                # Reconstruct dict format from commit log: value is the actual value string
                                value = entry["value"]
                                self.data[entry["key"]] = {"value": value, "timestamp": timestamp}
                logging.info(f"[Node {self.node_id}] Replayed commit log")
            except Exception as e:
                logging.warning(f"[Node {self.node_id}] Failed to replay commit log: {e}")

    def _local_get(self, key):
        """Get value for key, returning just the value string (extracted from dict format).
        Also performs expiration of expired tombstones during reads.
        """
        entry = self.data.get(key, None)
        if entry is None:
            logging.info(f"[Node {self.node_id}] Lookup key={key} -> None")
            return None
        # Extract value from the format: {"value": ..., "timestamp": ...} or {"deleted": True, ...}
        if isinstance(entry, dict):
            if entry.get("deleted"):
                # Check if tombstone is expired (lazy expiration)
                if self._is_tombstone_expired(entry.get("timestamp", 0)):
                    # Remove expired tombstone
                    del self.data[key]
                    logging.info(f"[Node {self.node_id}] Removed expired tombstone for key={key} during read")
                    return None
                logging.info(f"[Node {self.node_id}] Lookup key={key} -> None (deleted)")
                return None
            val = entry.get("value")
            logging.info(f"[Node {self.node_id}] Lookup key={key} -> {val}")
            return val
        else:
            # Fallback for unexpected format
            logging.info(f"[Node {self.node_id}] Lookup key={key} -> {entry}")
            return entry

    def _replicate(self, key, value, timestamp=None):
        replicas = self.ring.get_replicas(key, self.replication_factor)
        for replica in replicas:
            if replica.node_id == self.node_id:
                continue
            logging.info(f"[Node {self.node_id}] Sending REPLICATE for key={key} to {replica.node_id}")
            self.network.send(replica.node_id, {
                "type": "REPLICATE",
                "key": key,
                "value": value,
                "timestamp": timestamp,
                "from": self.node_id,
            })

    def _local_delete(self, key, timestamp=None):
        """Mark key as deleted and write to commit log."""
        if timestamp is None:
            timestamp = time.time()
        current = self.data.get(key)
        current_ts = current.get("timestamp", 0) if isinstance(current, dict) else 0
        
        # Clean up expired tombstone if present
        if isinstance(current, dict) and current.get("deleted") and self._is_tombstone_expired(current_ts):
            del self.data[key]
            current_ts = 0  # Reset after removing expired tombstone
            logging.info(f"[Node {self.node_id}] Removed expired tombstone for key={key} before new delete")
        
        # Tombstone: store deletion marker instead of removing immediately
        if timestamp > current_ts:
            self.data[key] = {"deleted": True, "timestamp": timestamp}
            self._append_commit_log(key, None, mutation_type="DELETE", timestamp=timestamp)
            logging.info(f"[Node {self.node_id}] Deleted key={key}, ts={timestamp}")
        elif timestamp == current_ts:
            # Idempotent duplicate - don't update or log
            logging.info(f"[Node {self.node_id}] Duplicate for key={key}, ts={timestamp} - skipping")

    def _replicate_delete(self, key, timestamp=None):
        replicas = self.ring.get_replicas(key, self.replication_factor)
        for replica in replicas:
            if replica.node_id == self.node_id:
                continue
            logging.info(f"[Node {self.node_id}] Sending REPLICATE_DELETE for key={key} to {replica.node_id}")
            self.network.send(replica.node_id, {
                "type": "REPLICATE_DELETE",
                "key": key,
                "timestamp": timestamp,
                "from": self.node_id,
            })

    def fail(self):
        """Simulate node failure (drops/ignores all incoming)."""
        if self.alive:
            logging.info(f"[Node {self.node_id}] FAIL")
        self.alive = False

    def recover(self):
        """Node recovery: load from disk, then request state from successor."""
        was_dead = not self.alive
        self.alive = True
        if was_dead:
            logging.info(f"[Node {self.node_id}] RECOVER -> loading from disk")
            # First, recover what we have on disk
            self._recover_from_disk()
            # Then request missing state from network
            logging.info(f"[Node {self.node_id}] RECOVER -> requesting snapshot from peers")
            successors = self.ring.get_successors(self.node_id, 1)
            if successors:
                succ = successors[0]
                self.network.send(succ.node_id, {
                    "type": "SNAPSHOT_REQUEST",
                    "from": self.node_id,
                })
            # Resume periodic snapshots
            self._schedule_snapshot()

    def on_receive(self, msg):
        """Handle incoming messages."""
        if not self.alive:
            return
        mtype = msg["type"]
        if mtype == "PUT":
            timestamp = time.time()
            self._local_put(msg["key"], msg["value"], timestamp=timestamp)
            # Replicate to successors
            self._replicate(msg["key"], msg["value"], timestamp=timestamp)
        elif mtype == "GET":
            val = self._local_get(msg["key"])
            # send back to requester
            self.network.send(msg["from"], {
                "type": "GET_RESPONSE",
                "key": msg["key"],
                "value": val,
                "from": self.node_id,
            })
        elif mtype == "GET_RESPONSE":
            logging.info(f"[Node {self.node_id}] Received GET_RESPONSE: {msg}")
        elif mtype == "REPLICATE":
            logging.info(f"[Node {self.node_id}] Received REPLICATE for key={msg['key']} from {msg['from']}")
            self._local_put(msg["key"], msg["value"], timestamp=msg.get("timestamp"))
        elif mtype == "SNAPSHOT_REQUEST":
            # Send full data snapshot to requester
            self.network.send(msg["from"], {
                "type": "SNAPSHOT_RESPONSE",
                "data": dict(self.data),
                "from": self.node_id,
            })
        elif mtype == "SNAPSHOT_RESPONSE":
            # Merge snapshot with timestamp-aware merging
            incoming = msg.get("data", {})
            merged_count = 0
            overwritten_count = 0
            skipped_count = 0
            for key, value in incoming.items():
                if key not in self.data:
                    # New key, just add it
                    self.data[key] = value
                    merged_count += 1
                else:
                    # Existing key, compare timestamps
                    current = self.data.get(key)
                    incoming_val = value
                    
                    # Get timestamps safely
                    current_ts = current.get("timestamp", 0) if isinstance(current, dict) else 0
                    incoming_ts = incoming_val.get("timestamp", 0) if isinstance(incoming_val, dict) else 0
                    
                    # Only update if incoming is newer (or equal for idempotency)
                    if incoming_ts > current_ts:
                        self.data[key] = value
                        overwritten_count += 1
                        merged_count += 1
                    elif incoming_ts == current_ts:
                        # Same timestamp - idempotent duplicate, skip silently
                        skipped_count += 1
                    else:
                        # Older timestamp - skip
                        skipped_count += 1
            
            if skipped_count > 0:
                logging.info(f"[Node {self.node_id}] Applied snapshot: {len(incoming)} keys from {msg['from']}, merged={merged_count}, overwritten={overwritten_count}, skipped={skipped_count}")
            else:
                logging.info(f"[Node {self.node_id}] Applied snapshot: {len(incoming)} keys from {msg['from']}, merged={merged_count}, overwritten={overwritten_count}")
        elif mtype == "DELETE":
            timestamp = time.time()
            self._local_delete(msg["key"], timestamp=timestamp)
            self._replicate_delete(msg["key"], timestamp=timestamp)

        elif mtype == "REPLICATE_DELETE":
            self._local_delete(msg["key"], timestamp=msg["timestamp"])


    def _is_tombstone_expired(self, tombstone_timestamp: float) -> bool:
        """Check if tombstone is older than gc_grace_seconds."""
        return (time.time() - tombstone_timestamp) > self.grace_period_seconds
    
    def _compact_tombstones(self):
        """Remove expired tombstones from in-memory store."""
        current_time = time.time()
        expired_keys = []
        for key, entry in self.data.items():
            if isinstance(entry, dict) and entry.get("deleted"):
                tombstone_ts = entry.get("timestamp", 0)
                if (current_time - tombstone_ts) > self.grace_period_seconds:
                    expired_keys.append(key)
        
        for key in expired_keys:
            del self.data[key]
            logging.info(f"[Node {self.node_id}] Removed expired tombstone for key={key}")

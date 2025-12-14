import logging
import uuid
import time
from typing import Dict, Optional

from .gossip import GossipProtocol
from .storage import StorageManager

class Node:
    """Represents a database node in the distributed ring."""

    def __init__(self, node_id=None, network=None, scheduler=None, ring=None, 
                 replication_factor: int = 2, snapshot_interval_ms: int = 500, 
                 log_dir: str = "logs", use_uuid: bool = True, grace_period_seconds: int = 864000,
                 gossip_interval_ms: int = 1000, failure_timeout_ms: int = 10000):
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
        
        self.storage = StorageManager(
            self.node_id, 
            log_dir=log_dir,
            snapshot_interval_ms=snapshot_interval_ms,
            grace_period_seconds=grace_period_seconds
        )
        
        self.gossip = GossipProtocol(
            self.node_id,
            network=network,
            scheduler=scheduler,
            ring=ring,
            gossip_interval_ms=gossip_interval_ms,
            failure_timeout_ms=failure_timeout_ms
        )
        
        self.data = self.storage.recover_from_disk()
        
        self._schedule_snapshot()
        
        if self.alive and scheduler:
            self.gossip.start()

        if network:
            network.register(self.node_id, self.on_receive)

    def put(self, key, value):
        """
        Store key in the correct node.
        
        Note: In consistent hashing, keys are routed to their primary node based on hash.
        Only the primary node and its replicas store the data. If the calling node is not
        the primary, it forwards the request to the primary node.
        """
        target = self.ring.get_node(key)
        if target == self:
            logging.info(f"[Node {self.node_id}] PUT {key}={value} -> storing locally (I am primary)")
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
        """Retrieve key from correct node."""
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
        """Delete key from the system."""
        target = self.ring.get_node(key)
        if target == self:
            timestamp = time.time()
            self._local_delete(key, timestamp=timestamp)
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
            self.storage.append_commit_log(key, value, mutation_type="PUT", timestamp=timestamp)
            logging.info(f"[Node {self.node_id}] Stored key={key}, value={value}, ts={timestamp}")
        elif timestamp == current_ts:
            # Idempotent duplicate - don't update or log
            logging.info(f"[Node {self.node_id}] Duplicate for key={key}, value={value}, ts={timestamp} - skipping")

    
    def _schedule_snapshot(self):
        """Schedule periodic snapshot writes."""
        if self.alive and self.scheduler:
            self.scheduler.call_later(self.storage.snapshot_interval_ms, self._take_snapshot)
    
    def _take_snapshot(self):
        """Take a snapshot and schedule the next one."""
        if self.alive:
            self.storage.write_snapshot(self.data)
            self._schedule_snapshot()

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
                if self.storage.is_tombstone_expired(entry.get("timestamp", 0)):
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
            # Only replicate to alive nodes
            if not replica.alive:
                logging.warning(f"[Node {self.node_id}] Skipping replication to dead replica {replica.node_id}")
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
        if isinstance(current, dict) and current.get("deleted") and self.storage.is_tombstone_expired(current_ts):
            del self.data[key]
            current_ts = 0  # Reset after removing expired tombstone
            logging.info(f"[Node {self.node_id}] Removed expired tombstone for key={key} before new delete")
        
        # Tombstone: store deletion marker instead of removing immediately
        if timestamp > current_ts:
            self.data[key] = {"deleted": True, "timestamp": timestamp}
            self.storage.append_commit_log(key, None, mutation_type="DELETE", timestamp=timestamp)
            logging.info(f"[Node {self.node_id}] Deleted key={key}, ts={timestamp}")
        elif timestamp == current_ts:
            # Idempotent duplicate - don't update or log
            logging.info(f"[Node {self.node_id}] Duplicate for key={key}, ts={timestamp} - skipping")

    def _replicate_delete(self, key, timestamp=None):
        """
        Replicate delete to current replica set, plus any nodes that have the key.
    
        """
        replicas = self.ring.get_replicas(key, self.replication_factor)
        replica_ids = {r.node_id for r in replicas}
        
        # Send delete to all current replicas
        for replica in replicas:
            if replica.node_id == self.node_id:
                continue
            # Only replicate to alive nodes
            if not replica.alive:
                logging.warning(f"[Node {self.node_id}] Skipping replication delete to dead replica {replica.node_id}")
                continue
            logging.info(f"[Node {self.node_id}] Sending REPLICATE_DELETE for key={key} to replica {replica.node_id}")
            self.network.send(replica.node_id, {
                "type": "REPLICATE_DELETE",
                "key": key,
                "timestamp": timestamp,
                "from": self.node_id,
            })
        
        # Also send to nodes that have the key but aren't in current replica set
        for node_id, node in self.ring.nodes.items():
            if node_id == self.node_id:
                continue
            if node_id in replica_ids:
                continue
            if not node.alive:
                continue
            # Check if this node has the key
            if key in node.data:
                logging.info(f"[Node {self.node_id}] Sending REPLICATE_DELETE for key={key} to {node_id} (has orphaned copy)")
                self.network.send(node_id, {
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
        self.gossip.mark_node_down(self.node_id)

    def recover(self):
        """Node recovery: load from disk, then request state from successor."""
        was_dead = not self.alive
        self.alive = True
        if was_dead:
            logging.info(f"[Node {self.node_id}] RECOVER -> loading from disk")
            self.data = self.storage.recover_from_disk()
            # Update gossip state to reflect we're back up
            self.gossip.mark_node_up(self.node_id)
            # Request missing state from network
            logging.info(f"[Node {self.node_id}] RECOVER -> requesting snapshot from peers")
            successors = self.ring.get_successors(self.node_id, 1)
            # Try to find an alive successor for snapshot request
            for succ in successors:
                if succ.alive:
                    self.network.send(succ.node_id, {
                        "type": "SNAPSHOT_REQUEST",
                        "from": self.node_id,
                    })
                    break
            else:
                logging.warning(f"[Node {self.node_id}] No alive successors available for snapshot recovery")
            self._schedule_snapshot()
            if self.scheduler:
                self.gossip.start()

    def on_receive(self, msg):
        """Handle incoming messages."""
        if not self.alive:
            return
        mtype = msg["type"]
        if mtype == "PUT":
            timestamp = time.time()
            self._local_put(msg["key"], msg["value"], timestamp=timestamp)
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
                    
                    current_ts = current.get("timestamp", 0) if isinstance(current, dict) else 0
                    incoming_ts = incoming_val.get("timestamp", 0) if isinstance(incoming_val, dict) else 0
                    
                    # Only update if incoming is newer (or equal for idempotency)
                    if incoming_ts > current_ts:
                        self.data[key] = value
                        overwritten_count += 1
                        merged_count += 1
                    elif incoming_ts == current_ts:
                        # Idempotent duplicate
                        skipped_count += 1
                    else:
                        # Older timestamp - skip
                        skipped_count += 1
            
            if skipped_count > 0:
                logging.info(f"[Node {self.node_id}] Applied snapshot: {len(incoming)} keys from {msg['from']}, merged={merged_count}, overwritten={overwritten_count}, skipped={skipped_count}")
            else:
                logging.info(f"[Node {self.node_id}] Applied snapshot: {len(incoming)} keys from {msg['from']}, merged={merged_count}, overwritten={overwritten_count}")
        elif mtype == "DELETE":
            # Primary node generates authoritative timestamp for client requests
            timestamp = time.time()
            self._local_delete(msg["key"], timestamp=timestamp)
            self._replicate_delete(msg["key"], timestamp=timestamp)

        elif mtype == "REPLICATE_DELETE":
            self._local_delete(msg["key"], timestamp=msg["timestamp"])
        elif mtype == "GOSSIP":
            self.gossip.handle_gossip_message(msg)

import logging
import json
import time
from pathlib import Path
from typing import Dict, Optional


class StorageManager:
    """Manages data persistence: commit logs, snapshots, and recovery."""
    
    def __init__(self, node_id: str, log_dir: str = "logs", 
                 snapshot_interval_ms: int = 500, grace_period_seconds: int = 864000):
        """
        Initialize storage manager.
        
        Args:
            node_id: Unique identifier for this node
            log_dir: Base directory for storing logs
            snapshot_interval_ms: How often to write snapshots
            grace_period_seconds: Grace period for tombstone expiration
        """
        self.node_id = node_id
        self.snapshot_interval_ms = snapshot_interval_ms
        self.grace_period_seconds = grace_period_seconds
        
        # Setup disk storage paths
        self.log_dir = Path(log_dir) / node_id
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.commit_log_path = self.log_dir / "commit.log"
        self.snapshot_path = self.log_dir / "snapshot.json"
        
        if not self.commit_log_path.exists():
            self.commit_log_path.touch()
    
    def append_commit_log(self, key: str, value: Optional[str], 
                         mutation_type: str = "PUT", timestamp: Optional[float] = None):
        """Append a mutation to the commit log."""
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
            logging.error(f"[Storage {self.node_id}] Failed to write commit log: {e}")
    
    def write_snapshot(self, data: Dict):
        """Write current in-memory state to disk snapshot.
        
        Compacts expired tombstones before writing snapshot.
        """
        compacted_data = self._compact_tombstones(data)
        
        try:
            temp_path = self.snapshot_path.with_suffix('.tmp')
            with open(temp_path, 'w') as f:
                json.dump(compacted_data, f, indent=2)
            temp_path.replace(self.snapshot_path)
            logging.info(f"[Storage {self.node_id}] Snapshot written: {len(compacted_data)} keys")
        except Exception as e:
            logging.error(f"[Storage {self.node_id}] Failed to write snapshot: {e}")
    
    def recover_from_disk(self) -> Dict:
        """Recover node state from disk: load snapshot then replay commit log.
        
        Returns:
            Dictionary of recovered data
        """
        data = {}
        
        # Load snapshot if it exists
        if self.snapshot_path.exists():
            try:
                with open(self.snapshot_path, 'r') as f:
                    data = json.load(f)
                logging.info(f"[Storage {self.node_id}] Loaded snapshot: {len(data)} keys")
            except Exception as e:
                logging.warning(f"[Storage {self.node_id}] Failed to load snapshot: {e}")
                data = {}
        
        # Replay commit log entries after snapshot
        if self.commit_log_path.exists():
            try:
                with open(self.commit_log_path, 'r') as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line)
                            timestamp = entry.get("timestamp", time.time())
                            if entry.get("type") == "DELETE":
                                data[entry["key"]] = {"deleted": True, "timestamp": timestamp}
                            else:
                                value = entry["value"]
                                data[entry["key"]] = {"value": value, "timestamp": timestamp}
                logging.info(f"[Storage {self.node_id}] Replayed commit log")
            except Exception as e:
                logging.warning(f"[Storage {self.node_id}] Failed to replay commit log: {e}")
        
        return data
    
    def is_tombstone_expired(self, tombstone_timestamp: float) -> bool:
        """Check if tombstone is older than gc_grace_seconds."""
        return (time.time() - tombstone_timestamp) > self.grace_period_seconds
    
    def _compact_tombstones(self, data: Dict) -> Dict:
        """Remove expired tombstones from data dictionary."""
        current_time = time.time()
        compacted = {}
        
        for key, entry in data.items():
            if isinstance(entry, dict) and entry.get("deleted"):
                tombstone_ts = entry.get("timestamp", 0)
                if (current_time - tombstone_ts) > self.grace_period_seconds:
                    logging.info(f"[Storage {self.node_id}] Removed expired tombstone for key={key}")
                    continue
            compacted[key] = entry
        
        return compacted


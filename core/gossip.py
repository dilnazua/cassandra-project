"""
Gossip Protocol implementation for cluster membership and failure detection.

The gossip protocol allows nodes to discover each other, exchange membership
information, and detect node failures in a decentralized manner.
"""
import logging
import random
import time
from typing import Dict, Optional, Tuple


class GossipProtocol:
    def __init__(self, node_id: str, network, scheduler, ring, 
                 gossip_interval_ms: int = 1000, failure_timeout_ms: int = 10000):
        """
        Args:
            node_id: Unique identifier for this node
            network: Network interface for sending messages
            scheduler: Scheduler for timing gossip exchanges
            ring: Ring object to update based on membership changes
            gossip_interval_ms: How often to gossip (default: 1000ms)
            failure_timeout_ms: Time before marking node as DOWN (default: 10000ms)
        """
        self.node_id = node_id
        self.network = network
        self.scheduler = scheduler
        self.ring = ring
        self.gossip_interval_ms = gossip_interval_ms
        self.failure_timeout_ms = failure_timeout_ms
        
        # Gossip state: node_id -> (heartbeat_version, state, timestamp_ms)
        # state: 'UP', 'DOWN', 'LEAVING', 'LEFT'
        self.gossip_state: Dict[str, Tuple[int, str, int]] = {}
        
        current_time = self._get_current_time_ms()
        self.gossip_state[self.node_id] = (1, 'UP', current_time)
    
    def start(self):
        """Start the gossip protocol by scheduling periodic gossip exchanges."""
        if self.scheduler:
            logging.info(f"[Gossip {self.node_id}] Starting gossip protocol (interval={self.gossip_interval_ms}ms)")
            self._gossip()
        else:
            logging.warning(f"[Gossip {self.node_id}] Cannot start gossip: no scheduler available")
    
    
    def mark_node_down(self, node_id: str):
        """Mark a node as DOWN in gossip state."""
        current_time = self._get_current_time_ms()
        version, _, _ = self.gossip_state.get(node_id, (0, 'UP', 0))
        logging.info(f"[Gossip {self.node_id}] Marking {node_id} as DOWN")
        self.gossip_state[node_id] = (version, 'DOWN', current_time)
        self._update_ring_from_gossip()
    
    def mark_node_up(self, node_id: str):
        """Mark a node as UP in gossip state."""
        current_time = self._get_current_time_ms()
        version, _, _ = self.gossip_state.get(node_id, (0, 'UP', 0))
        logging.info(f"[Gossip {self.node_id}] Marking {node_id} as UP (version {version + 1})")
        self.gossip_state[node_id] = (version + 1, 'UP', current_time)
        self._update_ring_from_gossip()
    
    def handle_gossip_message(self, msg: Dict):
        """Handle incoming gossip message: merge membership information."""
        incoming_state = msg.get("gossip_state", {})
        from_node = msg.get("from")
        
        if not from_node:
            return
        
        logging.info(f"[Gossip {self.node_id}] Received GOSSIP from {from_node}")
        updated = self._merge_gossip_state(incoming_state)
        if updated:
            logging.info(f"[Gossip {self.node_id}] Gossip state updated after merge with {from_node}")
            self._update_ring_from_gossip()
        else:
            logging.debug(f"[Gossip {self.node_id}] Gossip state unchanged after merge with {from_node}")
    
    def get_membership_state(self) -> Dict[str, Tuple[int, str, int]]:
        """Get current membership state."""
        return dict(self.gossip_state)
    
    def is_node_alive(self, node_id: str) -> bool:
        """Check if a node is considered alive based on gossip state."""
        entry = self.gossip_state.get(node_id)
        if entry is None:
            return False
        _, state, _ = entry
        return state == 'UP'
    
    # Private methods
    
    def _get_current_time_ms(self) -> int:
        """Get current time in milliseconds."""
        if self.scheduler and hasattr(self.scheduler, 'clock'):
            return self.scheduler.clock.now_ms()
        return int(time.time() * 1000)
    
    def _gossip(self):
        """Perform one gossip exchange: select a peer and exchange membership information."""
        # Don't gossip if this node is dead
        if self.ring and self.node_id in self.ring.nodes:
            if not self.ring.nodes[self.node_id].alive:
                logging.debug(f"[Gossip {self.node_id}] Skipping gossip - node is dead")
                # Still schedule next gossip in case node recovers
                if self.scheduler:
                    self.scheduler.call_later(self.gossip_interval_ms, self._gossip)
                return
        
        current_time = self._get_current_time_ms()
        version, _, _ = self.gossip_state.get(self.node_id, (0, 'UP', 0))
        self.gossip_state[self.node_id] = (version + 1, 'UP', current_time)
        
        peer = self._select_gossip_peer()
        if peer:
            logging.info(f"[Gossip {self.node_id}] Sending GOSSIP to {peer} (version {version + 1})")
            self.network.send(peer, {
                "type": "GOSSIP",
                "gossip_state": dict(self.gossip_state),
                "from": self.node_id,
            })
        else:
            logging.debug(f"[Gossip {self.node_id}] No peer available for gossip")
        
        self._check_failures()
        
        if self.scheduler:
            self.scheduler.call_later(self.gossip_interval_ms, self._gossip)
    
    def _select_gossip_peer(self) -> Optional[str]:
        """Select a random peer from known nodes (excluding self)."""
        if not self.ring:
            return None
        
        all_nodes = list(self.ring.nodes.keys())
        if len(all_nodes) <= 1:
            return None
        
        alive_peers = [
            node_id for node_id in all_nodes
            if node_id != self.node_id
            and self.gossip_state.get(node_id, (0, 'DOWN', 0))[1] == 'UP'
        ]
        
        if not alive_peers:
            alive_peers = [node_id for node_id in all_nodes if node_id != self.node_id]
        
        return random.choice(alive_peers) if alive_peers else None
    
    def _merge_gossip_state(self, incoming_state: Dict) -> bool:
        """Merge incoming gossip state with local state. Returns True if state changed."""
        updated = False
        current_time = self._get_current_time_ms()
        
        our_version, _, _ = self.gossip_state.get(self.node_id, (0, 'UP', 0))
        self.gossip_state[self.node_id] = (our_version, 'UP', current_time)
        
        for node_id, entry in incoming_state.items():
            if node_id == self.node_id:
                continue
            
            if not (isinstance(entry, (tuple, list)) and len(entry) == 3):
                continue
            
            incoming_version, incoming_state_str, incoming_timestamp = entry
            current_entry = self.gossip_state.get(node_id)
            
            if current_entry is None:
                self.gossip_state[node_id] = (incoming_version, incoming_state_str, incoming_timestamp)
                updated = True
            else:
                current_version, current_state, current_timestamp = current_entry
                if (incoming_version > current_version or 
                    (incoming_version == current_version and 
                     (incoming_timestamp > current_timestamp or incoming_state_str != current_state))):
                    self.gossip_state[node_id] = (incoming_version, incoming_state_str, incoming_timestamp)
                    updated = True
        
        return updated
    
    def _check_failures(self):
        """Check gossip state for nodes that should be marked as DOWN due to timeout."""
        if not self.ring:
            return
        
        current_time = self._get_current_time_ms()
        updated = False
        
        for node_id, (version, state, timestamp) in list(self.gossip_state.items()):
            if node_id == self.node_id:
                continue
            
            if state == 'UP' and (current_time - timestamp) > self.failure_timeout_ms:
                logging.info(f"[Gossip {self.node_id}] Marking {node_id} as DOWN (timeout: {current_time - timestamp}ms > {self.failure_timeout_ms}ms)")
                self.gossip_state[node_id] = (version, 'DOWN', timestamp)
                updated = True
                if node_id in self.ring.nodes:
                    self.ring.nodes[node_id].alive = False
        
        if updated:
            self._update_ring_from_gossip()
    
    def _update_ring_from_gossip(self):
        """Update ring membership based on gossip state.
        
        Note: This will mark nodes as DOWN based on gossip, but will not
        automatically recover nodes that are currently dead. Nodes must be
        explicitly recovered via node.recover().
        """
        if not self.ring:
            return
        
        for node_id in list(self.ring.nodes.keys()):
            if node_id == self.node_id:
                continue
            
            gossip_entry = self.gossip_state.get(node_id)
            if gossip_entry:
                _, state, _ = gossip_entry
                node = self.ring.nodes[node_id]
                # Only update to UP if gossip says UP AND node is currently alive
                # This prevents gossip from auto-recovering manually failed nodes
                # Gossip can still mark nodes as DOWN based on timeout
                if state == 'UP' and node.alive:
                    pass
                elif state == 'DOWN':
                    node.alive = False
                

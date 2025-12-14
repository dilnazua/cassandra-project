import hashlib
import bisect

class Ring:
    """Simple consistent hashing ring for node placement and key lookup."""

    def __init__(self):
        self.ring = []       # Sorted list of (hash, node_id)
        self.nodes = {}      # node_id -> node reference

    def _hash(self, key: str) -> int:
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**32)

    def add_node(self, node_id: str, node_ref):
        h = self._hash(node_id)
        bisect.insort(self.ring, (h, node_id))
        self.nodes[node_id] = node_ref

    def remove_node(self, node_id: str):
        self.ring = [(h, n) for (h, n) in self.ring if n != node_id]
        self.nodes.pop(node_id, None)

    def get_node(self, key: str):
        """Return node responsible for this key, skipping dead nodes."""
        if not self.ring:
            return None
        h = self._hash(key)
        i = bisect.bisect(self.ring, (h,))
        if i == len(self.ring):
            i = 0
        
        # Try to find an alive node, wrapping around if necessary
        start_i = i
        while True:
            _, node_id = self.ring[i]
            node = self.nodes[node_id]
            if node.alive:
                return node
            # Move to next node
            i = (i + 1) % len(self.ring)
            # If we've wrapped around, no alive nodes exist
            if i == start_i:
                return node

    def get_node_id(self, key: str) -> str:
        """Return node_id responsible for this key, skipping dead nodes."""
        if not self.ring:
            return None
        h = self._hash(key)
        i = bisect.bisect(self.ring, (h,))
        if i == len(self.ring):
            i = 0
        
        # Try to find an alive node, wrapping around if necessary
        start_i = i
        while True:
            _, node_id = self.ring[i]
            node = self.nodes[node_id]
            if node.alive:
                return node_id
            # Move to next node
            i = (i + 1) % len(self.ring)
            # If we've wrapped around, no alive nodes exist
            if i == start_i:
                return node_id

    def get_successors(self, node_id: str, count: int):
        """Return up to `count` alive successors on the ring starting after node_id.

        Includes wrap-around. Excludes the given node_id. Only returns alive nodes.
        """
        if not self.ring or count <= 0:
            return []
        try:
            idx = next(i for i, (_, nid) in enumerate(self.ring) if nid == node_id)
        except StopIteration:
            return []
        out = []
        j = (idx + 1) % len(self.ring)
        start_j = j
        # Keep searching until we have enough alive nodes or we've wrapped around
        while len(out) < count:
            if j == start_j and len(out) > 0:
                # We've wrapped around and found some nodes, stop
                break
            _, nid = self.ring[j]
            if nid != node_id:
                node = self.nodes[nid]
                if node.alive:
                    out.append(node)
            j = (j + 1) % len(self.ring)
            # Safety check: if we've wrapped around without finding any nodes, break
            if j == start_j and len(out) == 0:
                break
        return out

    def get_replicas(self, key: str, rf: int):
        """Return up to `rf` alive replica node refs for this key.
        
        Only returns alive nodes. If primary is dead, finds next alive node as primary.
        """
        if not self.ring:
            return []
        primary_id = self.get_node_id(key)
        if primary_id is None:
            return []
        primary = self.nodes[primary_id]
        
        # If primary is dead, we need to find the next alive node
        # get_node_id already handles this, but we need to ensure primary is alive
        if not primary.alive:
            h = self._hash(key)
            i = bisect.bisect(self.ring, (h,))
            if i == len(self.ring):
                i = 0
            start_i = i
            while True:
                _, node_id = self.ring[i]
                node = self.nodes[node_id]
                if node.alive:
                    primary = node
                    primary_id = node_id
                    break
                i = (i + 1) % len(self.ring)
                if i == start_i:
                    return []
        
        if rf <= 1:
            return [primary] if primary.alive else []
        
        successors = self.get_successors(primary_id, rf - 1)
        return [primary] + successors

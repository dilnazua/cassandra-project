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
        """Return node responsible for this key."""
        if not self.ring:
            return None
        h = self._hash(key)
        i = bisect.bisect(self.ring, (h,))
        if i == len(self.ring):
            i = 0
        _, node_id = self.ring[i]
        return self.nodes[node_id]

    def get_node_id(self, key: str) -> str:
        """Return node_id responsible for this key."""
        if not self.ring:
            return None
        h = self._hash(key)
        i = bisect.bisect(self.ring, (h,))
        if i == len(self.ring):
            i = 0
        _, node_id = self.ring[i]
        return node_id

    def get_successors(self, node_id: str, count: int):
        """Return up to `count` successors on the ring starting after node_id.

        Includes wrap-around. Excludes the given node_id.
        """
        if not self.ring or count <= 0:
            return []
        # Find index of node_id in ring
        try:
            idx = next(i for i, (_, nid) in enumerate(self.ring) if nid == node_id)
        except StopIteration:
            return []
        out = []
        j = (idx + 1) % len(self.ring)
        while len(out) < min(count, len(self.ring) - 1):
            _, nid = self.ring[j]
            if nid != node_id:
                out.append(self.nodes[nid])
            j = (j + 1) % len(self.ring)
        return out

    def get_replicas(self, key: str, rf: int):
        """Return up to `rf` replica node refs for this key (primary + successors)."""
        if not self.ring:
            return []
        primary_id = self.get_node_id(key)
        if primary_id is None:
            return []
        primary = self.nodes[primary_id]
        if rf <= 1 or len(self.ring) == 1:
            return [primary]
        successors = self.get_successors(primary_id, rf - 1)
        return [primary] + successors

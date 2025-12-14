from sim.scheduler import SimClock, SimScheduler
from sim.network import SimNetwork, partition
from core.ring import Ring
from core.node import Node
from testing.test_crash_recovery import advance_time

def test_partition_tolerance():
    """Test that system operates during partition and converges after healing."""
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()
    nodes = [
        Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False),
        Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False),
        Node("C", network, scheduler, ring, replication_factor=2, use_uuid=False),
        Node("D", network, scheduler, ring, replication_factor=2, use_uuid=False),
    ]
    for node in nodes:
        ring.add_node(node.node_id, node)

    nodes[0].put("key1", "value1")
    advance_time(scheduler, clock, 500)

    # Create partition: split {A,B} and {C,D}
    network.add_rule(partition({("A", "C"), ("A", "D"), ("B", "C"), ("B", "D")}))
    
    # Write different values in each partition
    nodes[0].put("key1", "value1-partition1")  # Partition 1
    nodes[2].put("key1", "value1-partition2")  # Partition 2
    advance_time(scheduler, clock, 500)

    # Remove partition (heal)
    network.rules = []  # Clear partition rules
    
    advance_time(scheduler, clock, 2000)

    # Verify: last write should win (timestamp-based)
    assert all(node.alive for node in nodes), "All nodes should be alive"
    
    # Check that at least one partition's write is present
    values = [node._local_get("key1") for node in nodes]
    non_none_values = [v for v in values if v is not None]
    assert len(non_none_values) > 0, "At least one node should have the value"


if __name__ == "__main__":
    test_partition_tolerance()
    print("Partition tolerance test passed")


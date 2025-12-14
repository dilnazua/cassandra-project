from sim.scheduler import SimClock, SimScheduler
from sim.network import SimNetwork
from core.ring import Ring
from core.node import Node
from testing.test_crash_recovery import advance_time


def test_gossip_failure_detection():
    """Test that gossip detects node failures via timeout."""
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()

    nodes = [
        Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False,
             gossip_interval_ms=500, failure_timeout_ms=2000),
        Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False,
             gossip_interval_ms=500, failure_timeout_ms=2000),
        Node("C", network, scheduler, ring, replication_factor=2, use_uuid=False,
             gossip_interval_ms=500, failure_timeout_ms=2000),
    ]
    
    for node in nodes:
        ring.add_node(node.node_id, node)
    advance_time(scheduler, clock, 1500)
    assert all(node.alive for node in nodes)
    nodes[1].fail()
    advance_time(scheduler, clock, 2500)
    assert not nodes[1].alive
    # Note: Gossip updates ring.alive, so we check the ring's view
    assert nodes[0].gossip.is_node_alive("B") == False
    assert nodes[2].gossip.is_node_alive("B") == False


def test_gossip_recovery_propagation():
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()

    nodes = [
        Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False,
             gossip_interval_ms=500, failure_timeout_ms=2000),
        Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False,
             gossip_interval_ms=500, failure_timeout_ms=2000),
    ]
    
    for node in nodes:
        ring.add_node(node.node_id, node)
    advance_time(scheduler, clock, 1000)
    # Fail and recover node B
    nodes[1].fail()
    advance_time(scheduler, clock, 500)
    nodes[1].recover()
    # Let gossip propagate recovery
    advance_time(scheduler, clock, 1500)

    assert nodes[1].alive
    assert nodes[0].gossip.is_node_alive("B") == True


def test_gossip_membership_discovery():
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()

    nodes = [
        Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False,
             gossip_interval_ms=500),
        Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False,
             gossip_interval_ms=500),
        Node("C", network, scheduler, ring, replication_factor=2, use_uuid=False,
             gossip_interval_ms=500),
    ]
    
    ring.add_node(nodes[0].node_id, nodes[0])
    advance_time(scheduler, clock, 500)
    
    ring.add_node(nodes[1].node_id, nodes[1])
    advance_time(scheduler, clock, 1000)
    
    ring.add_node(nodes[2].node_id, nodes[2])
    advance_time(scheduler, clock, 2000)

    # All nodes should know about each other via gossip
    for node in nodes:
        membership = node.gossip.get_membership_state()
        assert len(membership) == 3, f"Node {node.node_id} should know about all 3 nodes"


if __name__ == "__main__":
    test_gossip_failure_detection()
    print("Gossip failure detection test passed")
    test_gossip_recovery_propagation()
    print("Gossip recovery propagation test passed")
    test_gossip_membership_discovery()
    print("Gossip membership discovery test passed")


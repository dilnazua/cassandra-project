from sim.scheduler import SimClock, SimScheduler
from sim.network import SimNetwork
from core.ring import Ring
from core.node import Node
from testing.test_crash_recovery import advance_time


def test_replication_to_all_replicas():
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()

    nodes = [
        Node("A", network, scheduler, ring, replication_factor=3, use_uuid=False),
        Node("B", network, scheduler, ring, replication_factor=3, use_uuid=False),
        Node("C", network, scheduler, ring, replication_factor=3, use_uuid=False),
        Node("D", network, scheduler, ring, replication_factor=3, use_uuid=False),
    ]
    
    for node in nodes:
        ring.add_node(node.node_id, node)

    primary = ring.get_node("key1")
    primary.put("key1", "value1")
    advance_time(scheduler, clock, 1500)
    replicas = ring.get_replicas("key1", rf=3)
    assert len(replicas) == 3, "Should have 3 replicas"
    for replica in replicas:
        value = replica._local_get("key1")
        assert value == "value1", f"Replica node {replica.node_id} should have key1"


def test_replication_skips_dead_nodes():
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()

    nodes = [
        Node("A", network, scheduler, ring, replication_factor=3, use_uuid=False),
        Node("B", network, scheduler, ring, replication_factor=3, use_uuid=False),
        Node("C", network, scheduler, ring, replication_factor=3, use_uuid=False),
    ]
    
    for node in nodes:
        ring.add_node(node.node_id, node)

    # Fail one replica before write
    nodes[1].fail()
    advance_time(scheduler, clock, 100)

    nodes[0].put("key1", "value1")
    advance_time(scheduler, clock, 1500)
    replicas = ring.get_replicas("key1", rf=3)
    replica_ids = {replica.node_id for replica in replicas}
    
    # Check that all alive replicas have the value
    for node in nodes:
        value = node._local_get("key1")
        if node.node_id in replica_ids:
            # This node is an alive replica
            assert value == "value1", f"Alive replica node {node.node_id} should have key1"
        elif not node.alive:
            # Dead nodes won't receive replication, so they shouldn't have new data
            pass
        else:
            # This is an alive node but not a replica - shouldn't have the value
            assert value is None, f"Non-replica node {node.node_id} should not have key1"


def test_delete_replication():
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()

    nodes = [
        Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False),
        Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False),
        Node("C", network, scheduler, ring, replication_factor=2, use_uuid=False),
    ]
    
    for node in nodes:
        ring.add_node(node.node_id, node)

    nodes[0].put("key1", "value1")
    advance_time(scheduler, clock, 1500)
    
    nodes[0].delete("key1")
    advance_time(scheduler, clock, 1500)
    replicas = ring.get_replicas("key1", rf=2)
    replica_ids = {replica.node_id for replica in replicas}
    for node in nodes:
        value = node._local_get("key1")
        if node.node_id in replica_ids:
            assert value is None, f"Replica node {node.node_id} should show key1 as deleted"


if __name__ == "__main__":
    test_replication_to_all_replicas()
    print("Replication to all replicas test passed")
    test_replication_skips_dead_nodes()
    print("Replication skips dead nodes test passed")
    test_delete_replication()
    print("Delete replication test passed")


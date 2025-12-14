from sim.scheduler import SimClock, SimScheduler
from sim.network import SimNetwork
from core.ring import Ring
from core.node import Node
import os
import shutil


def advance_time(scheduler, clock, ms):
    for _ in range(ms // 50 + 1):
        scheduler.run_due()
        clock.advance(50)


def cleanup_logs(node_ids):
    for node_id in node_ids:
        log_dir = f"logs/{node_id}"
        if os.path.exists(log_dir):
            shutil.rmtree(log_dir)


def test_recovery_from_snapshot_and_log():
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()

    cleanup_logs(["A", "B"])
    
    nodes = [
        Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False,
             snapshot_interval_ms=1000),
        Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False),
    ]
    
    for node in nodes:
        ring.add_node(node.node_id, node)

    nodes[0].put("key1", "value1")
    nodes[0].put("key2", "value2")
    advance_time(scheduler, clock, 1500)

    # Write more data after snapshot
    nodes[0].put("key3", "value3")
    advance_time(scheduler, clock, 200)

    # Simulate crash: create new node with same ID
    nodes[0].fail()
    advance_time(scheduler, clock, 100)
    
    new_node = Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False,
                    snapshot_interval_ms=1000)
    ring.remove_node("A")
    ring.add_node("A", new_node)
    nodes[0] = new_node

    # Recovered node should have all data
    assert nodes[0]._local_get("key1") == "value1"
    assert nodes[0]._local_get("key2") == "value2"
    assert nodes[0]._local_get("key3") == "value3"

    cleanup_logs(["A", "B"])


def test_recovery_with_missing_state():
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    ring = Ring()
    cleanup_logs(["A", "B"])
    nodes = [
        Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False),
        Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False),
    ]
    for node in nodes:
        ring.add_node(node.node_id, node)

    # Write data that replicates to both nodes
    nodes[0].put("key1", "value1")
    advance_time(scheduler, clock, 1000)

    # Node B should have the replicated data
    assert nodes[1]._local_get("key1") == "value1"

    nodes[0].fail()
    advance_time(scheduler, clock, 100)
    
    new_node = Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False)
    ring.remove_node("A")
    ring.add_node("A", new_node)
    nodes[0] = new_node
    nodes[0].recover()  # Request snapshot from peers
    
    advance_time(scheduler, clock, 2000)

    # Recovered node should have received data from peer
    assert nodes[0]._local_get("key1") == "value1"

    cleanup_logs(["A", "B"])


if __name__ == "__main__":
    test_recovery_from_snapshot_and_log()
    print("Recovery from snapshot and log test passed")
    test_recovery_with_missing_state()
    print("Recovery with missing state test passed")


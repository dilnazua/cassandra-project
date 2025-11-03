from sim.scheduler import SimClock, SimScheduler
from sim.network import SimNetwork, drop, delay, duplicate
from core.ring import Ring
from core.node import Node
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")

def main():
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)

    # Add some network fault simulation
    network.add_rule(delay(50, 100))
    network.add_rule(drop(0.01))
    network.add_rule(duplicate(0.05))

    ring = Ring()

    n1 = Node("A", network, scheduler, ring, replication_factor=2, use_uuid=False)
    n2 = Node("B", network, scheduler, ring, replication_factor=2, use_uuid=False)
    n3 = Node("C", network, scheduler, ring, replication_factor=2, use_uuid=False)

    # Register nodes in ring
    for n in [n1, n2, n3]:
        ring.add_node(n.node_id, n)

    # Perform baseline operations
    n1.put("apple", "red")
    n2.put("banana", "yellow")
    n3.get("apple")

    # Run enough time for replication to complete (messages need time to deliver)
    # Network delay is 50-100ms + base delay, so we need more time
    for _ in range(10):
        scheduler.run_due()
        clock.advance(150)

    # Introduce a failure and a write during outage
    n2.fail()
    n1.put("cherry", "dark-red")

    # Advance time while node B is down (replicas should still carry data)
    for _ in range(6):
        scheduler.run_due()
        clock.advance(50)

    # Recover node B and let it request a snapshot
    n2.recover()

    for t in range(0, 800, 50):
        scheduler.run_due()
        clock.advance(50)

    print("\nFinal states:")
    for n in [n1, n2, n3]:
        print(f"Node {n.node_id}: {n.data}")
    print("\nNetwork stats:")
    print(network.stats)

if __name__ == "__main__":
    main()

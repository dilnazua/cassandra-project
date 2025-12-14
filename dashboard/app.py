import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from math import cos, sin, pi
import sys
import os
import bisect

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sim.scheduler import SimClock, SimScheduler
from sim.network import SimNetwork, drop, delay, duplicate
from core.ring import Ring
from core.node import Node

# Page config
st.set_page_config(page_title="Cassandra-like System Visualizer", layout="wide")

# Initialize session state
if 'initialized' not in st.session_state:
    st.session_state.initialized = False
    st.session_state.clock = None
    st.session_state.scheduler = None
    st.session_state.network = None
    st.session_state.ring = None
    st.session_state.nodes = []
    st.session_state.simulation_step = 0
    st.session_state.auto_run = False

def initialize_system():
    """Initialize the distributed system."""
    clock = SimClock()
    scheduler = SimScheduler(clock)
    network = SimNetwork(scheduler, seed=42)
    
    # Add network fault simulation
    network.add_rule(delay(50, 100))
    network.add_rule(drop(0.01))
    network.add_rule(duplicate(0.05))
    
    ring = Ring()
    
    n1 = Node("A", network, scheduler, ring, replication_factor=3, use_uuid=False)
    n2 = Node("D", network, scheduler, ring, replication_factor=3, use_uuid=False)
    n3 = Node("P", network, scheduler, ring, replication_factor=3, use_uuid=False)
    n4 = Node("G", network, scheduler, ring, replication_factor=3, use_uuid=False)
    n5 = Node("V", network, scheduler, ring, replication_factor=3, use_uuid=False)
    
    
    # Register nodes in ring
    for n in [n1, n2, n3, n4, n5]:
        ring.add_node(n.node_id, n)
    
    st.session_state.clock = clock
    st.session_state.scheduler = scheduler
    st.session_state.network = network
    st.session_state.ring = ring
    st.session_state.nodes = [n1, n2, n3, n4, n5]
    st.session_state.initialized = True
    st.session_state.simulation_step = 0

def draw_ring(ring, nodes, selected_key=None):
    """Draw the consistent hashing ring with nodes and keys, showing replication."""
    fig, ax = plt.subplots(figsize=(12, 12))
    ax.set_aspect('equal')
    ax.axis('off')
    
    center = (0, 0)
    radius = 3
    
    replication_factor = nodes[0].replication_factor if nodes else 3
    
    circle = plt.Circle(center, radius, fill=False, color='gray', linewidth=2, linestyle='--')
    ax.add_patch(circle)
    
    # Draw nodes on the ring
    node_positions = {}
    for hash_val, node_id in ring.ring:
        # Convert hash to angle (0 to 2π)
        angle = (hash_val / (2**32)) * 2 * pi
        x = center[0] + radius * cos(angle)
        y = center[1] + radius * sin(angle)
        node_positions[node_id] = (x, y, angle, hash_val)
        
        # Get node reference
        node = ring.nodes[node_id]
        color = 'green' if node.alive else 'red'
        
        # Draw node
        ax.plot(x, y, 'o', markersize=20, color=color, markeredgecolor='black', markeredgewidth=2)
        ax.text(x, y, node_id, ha='center', va='center', fontsize=14, fontweight='bold', color='white')
        
        # Draw line from center to node
        ax.plot([center[0], x], [center[1], y], 'k--', alpha=0.3, linewidth=1)
    
    # Draw keys on the ring
    all_keys = set()
    for node in nodes:
        all_keys.update(node.data.keys())
    
    for key in all_keys:
        # Get primary node and all replicas for this key
        primary_id = ring.get_node_id(key)
        if primary_id:
            # Hash the key
            key_hash = ring._hash(key)
            angle = (key_hash / (2**32)) * 2 * pi
            x = center[0] + (radius - 0.3) * cos(angle)
            y = center[1] + (radius - 0.3) * sin(angle)
            
            # Determine if this is the selected key
            is_selected = (selected_key == key)
            key_alpha = 1.0 if is_selected else 0.6
            key_size = 12 if is_selected else 8
            
            # Draw key marker
            ax.plot(x, y, 's', markersize=key_size, color='blue', alpha=key_alpha, 
                   markeredgecolor='darkblue', markeredgewidth=2 if is_selected else 1)
            ax.text(x + 0.25, y + 0.25, key, ha='left', va='bottom', fontsize=9 if is_selected else 8,
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.8, edgecolor='darkblue' if is_selected else None),
                   fontweight='bold' if is_selected else 'normal')
            
            # Get all replica nodes for this key (only alive nodes)
            replicas = ring.get_replicas(key, replication_factor)
            replica_ids = [r.node_id for r in replicas]
            
            # Check if primary node is the original (based on hash) or a reassigned one
            key_hash = ring._hash(key)
            original_primary_idx = bisect.bisect(ring.ring, (key_hash,))
            if original_primary_idx == len(ring.ring):
                original_primary_idx = 0
            _, original_primary_id = ring.ring[original_primary_idx]
            is_reassigned = (len(replica_ids) > 0 and replica_ids[0] != original_primary_id)
            
            # Draw lines to all replica nodes with different styles
            for i, replica_id in enumerate(replica_ids):
                replica_pos = node_positions.get(replica_id)
                if replica_pos:
                    px, py = replica_pos[0], replica_pos[1]
                    if i == 0:
                        # Primary node: solid bold line
                        # Use orange/red color if reassigned due to failure
                        line_color = 'orange' if is_reassigned else 'blue'
                        ax.plot([x, px], [y, py], '-', color=line_color, alpha=0.7, linewidth=2.5, 
                               label='Primary Node' if key == list(all_keys)[0] and i == 0 else '')
                        # Add arrow to show direction
                        ax.annotate('', xy=(px, py), xytext=(x, y),
                                  arrowprops=dict(arrowstyle='->', color=line_color, lw=2, alpha=0.7))
                    else:
                        # Replica nodes: dashed line
                        ax.plot([x, px], [y, py], 'b--', alpha=0.4, linewidth=1.5,
                               label='Replica Node' if key == list(all_keys)[0] and i == 1 else '')
                        # Add arrow to show direction
                        ax.annotate('', xy=(px, py), xytext=(x, y),
                                  arrowprops=dict(arrowstyle='->', color='blue', lw=1.5, 
                                                alpha=0.4, linestyle='dashed'))
                    
                    # Add replica number label near the node
                    if is_selected:
                        label_x = px + 0.25
                        label_y = py + 0.25
                        label_text = 'P' if i == 0 else f'R{i}'
                        if i == 0 and is_reassigned:
                            label_text = 'P*'  # Mark reassigned primary
                        ax.text(label_x, label_y, label_text, ha='center', va='center',
                               fontsize=8, fontweight='bold',
                               bbox=dict(boxstyle='round,pad=0.2', facecolor='cyan', alpha=0.7))
    
    ax.set_xlim(-5.5, 5.5)
    ax.set_ylim(-5.5, 5.5)
    ax.set_title(f'Consistent Hashing Ring (Replication Factor: {replication_factor})', 
                fontsize=16, fontweight='bold')
    
    # Add legends
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='o', color='w', markerfacecolor='green', 
               markersize=12, label='Alive Node', markeredgecolor='black', markeredgewidth=2),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='red', 
               markersize=12, label='Dead Node', markeredgecolor='black', markeredgewidth=2),
        Line2D([0], [0], marker='s', color='blue', markersize=10, label='Key Location', 
               markeredgecolor='darkblue', markeredgewidth=1, alpha=0.8),
        Line2D([0], [0], color='blue', linewidth=2.5, label='→ Primary Node (bold)'),
        Line2D([0], [0], color='blue', linewidth=1.5, linestyle='--', label='→ Replica Node (dashed)'),
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=10)
    
    # Add explanation text box
    explanation_text = (
        "Key Information:\n"
        "• Keys are placed on the ring based on hash\n"
        "• Solid arrow (→) points to PRIMARY node\n"
        "• Dashed arrow (⇢) points to REPLICA nodes\n"
        "• Data is replicated to RF nodes total\n"
        "• P = Primary, R1, R2... = Replicas"
    )
    ax.text(-5, -4.5, explanation_text, fontsize=9, 
           bbox=dict(boxstyle='round,pad=0.5', facecolor='wheat', alpha=0.8),
           verticalalignment='top', family='monospace')
    
    return fig

def get_node_data_table(nodes):
    """Create a table showing data in each node."""
    data = []
    for node in nodes:
        node_data = {}
        for key, value in node.data.items():
            if isinstance(value, dict):
                if value.get('deleted'):
                    node_data[key] = f"<DELETED> (ts: {value.get('timestamp', 'N/A')})"
                else:
                    node_data[key] = f"{value.get('value', 'N/A')} (ts: {value.get('timestamp', 'N/A')})"
            else:
                node_data[key] = str(value)
        data.append({
            'Node': node.node_id,
            'Status': 'Alive' if node.alive else 'Dead',
            'Data Count': len(node.data),
            'Data': node_data
        })
    return data

def main():
    st.title("Cassandra-like Distributed System Visualizer")
    
    # Sidebar controls
    with st.sidebar:
        st.header("Controls")
        
        if st.button("Initialize System", type="primary"):
            initialize_system()
            st.rerun()
        
        if not st.session_state.initialized:
            st.warning("Please initialize the system first.")
            return
        
        st.divider()
        st.subheader("System Info")
        if st.session_state.nodes:
            st.info(f"**Replication Factor:** {st.session_state.nodes[0].replication_factor}\n\n"
                   f"This means each key is stored on {st.session_state.nodes[0].replication_factor} nodes "
                   f"(1 primary + {st.session_state.nodes[0].replication_factor - 1} replicas)")
        
        st.divider()
        st.subheader("Simulation")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Step Forward"):
                st.session_state.scheduler.run_due()
                st.session_state.clock.advance(50)
                st.session_state.simulation_step += 1
                st.rerun()
        
        with col2:
            if st.button("Run 10 Steps"):
                for _ in range(10):
                    st.session_state.scheduler.run_due()
                    st.session_state.clock.advance(50)
                st.session_state.simulation_step += 10
                st.rerun()
        
        st.divider()
        st.subheader("Operations")
        
        operation = st.selectbox("Operation", ["PUT", "GET", "DELETE"])
        
        if operation == "PUT":
            key = st.text_input("Key", value="apple")
            value = st.text_input("Value", value="red")
            if st.button("Execute PUT"):
                st.session_state.nodes[0].put(key, value)
                st.rerun()
        
        elif operation == "GET":
            key = st.text_input("Key", value="apple")
            if st.button("Execute GET"):
                result = st.session_state.nodes[0].get(key)
                st.info(f"Result: {result}")
                st.rerun()
        
        elif operation == "DELETE":
            key = st.text_input("Key", value="apple")
            if st.button("Execute DELETE"):
                st.session_state.nodes[0].delete(key)
                st.rerun()
        
        st.divider()
        st.subheader("Node Control")
        
        node_select = st.selectbox("Select Node", [n.node_id for n in st.session_state.nodes])
        selected_node = next(n for n in st.session_state.nodes if n.node_id == node_select)
        
        if selected_node.alive:
            if st.button("Fail Node"):
                selected_node.fail()
                st.rerun()
        else:
            if st.button("Recover Node"):
                selected_node.recover()
                st.rerun()
    
    # Main content
    if not st.session_state.initialized:
        st.info("👈 Click 'Initialize System' in the sidebar to start.")
        return
    
    # System status
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Simulation Time", f"{st.session_state.clock.now_ms()} ms")
    with col2:
        st.metric("Simulation Steps", st.session_state.simulation_step)
    with col3:
        alive_count = sum(1 for n in st.session_state.nodes if n.alive)
        st.metric("Alive Nodes", f"{alive_count}/{len(st.session_state.nodes)}")
    with col4:
        total_data = sum(len(n.data) for n in st.session_state.nodes)
        st.metric("Total Keys", total_data)
    
    st.divider()
    
    # Ring visualization
    st.subheader("Ring Topology")
    
    # Key selection for highlighting
    all_keys = set()
    for node in st.session_state.nodes:
        all_keys.update(node.data.keys())
    
    selected_key_for_viz = None
    if all_keys:
        selected_key_for_viz = st.selectbox(
            "Select a key to highlight (shows replication):",
            options=["None"] + sorted(all_keys),
            key="key_selector"
        )
        if selected_key_for_viz == "None":
            selected_key_for_viz = None
    
    fig = draw_ring(st.session_state.ring, st.session_state.nodes, selected_key_for_viz)
    st.pyplot(fig)
    plt.close(fig)
    
    # Show detailed information for selected key
    if selected_key_for_viz:
        with st.expander(f"🔍 Details for key: '{selected_key_for_viz}'"):
            primary_id = st.session_state.ring.get_node_id(selected_key_for_viz)
            replicas = st.session_state.ring.get_replicas(selected_key_for_viz, st.session_state.nodes[0].replication_factor)
            replica_ids = [r.node_id for r in replicas]
            
            st.write(f"**Replication Factor:** {st.session_state.nodes[0].replication_factor}")
            primary_status = '🟢' if st.session_state.ring.nodes[primary_id].alive else '🔴'
            st.write(f"**Primary Node:** {primary_id} {primary_status}")
            
            replica_list = []
            for rid in replica_ids[1:]:
                replica_status = '🟢' if st.session_state.ring.nodes[rid].alive else '🔴'
                replica_list.append(f"{rid} ({replica_status})")
            st.write(f"**Replica Nodes:** {', '.join(replica_list)}")
            
            # Show where the key is stored
            st.write("**Storage locations:**")
            for replica in replicas:
                has_key = selected_key_for_viz in replica.data
                status = "Stored" if has_key else "Not stored"
                if has_key:
                    value_info = replica.data[selected_key_for_viz]
                    if isinstance(value_info, dict):
                        if value_info.get('deleted'):
                            status += f" (DELETED, ts: {value_info.get('timestamp', 'N/A')})"
                        else:
                            status += f" (value: {value_info.get('value', 'N/A')}, ts: {value_info.get('timestamp', 'N/A')})"
                st.write(f"- Node {replica.node_id}: {status}")
    
    # Operation Flow Explanation
    st.divider()
    st.subheader("How Operations Work")
    
    with st.expander("PUT Operation Flow", expanded=True):
        st.markdown("""
        **When you PUT a key-value pair:**
        
        1. **Hash & Route**: Key is hashed to find its position on the ring
        2. **Find Primary**: The ring identifies the PRIMARY node (next node clockwise from key's hash)
        3. **Store Locally**: Primary node stores the data locally
        4. **Replicate**: Primary node sends replication requests to RF-1 replica nodes (next nodes on ring)
        5. **Confirm**: All replica nodes store the data
        
        **Visual**: In the ring above, a key shows:
        - Key location on the ring (square marker)
        - → **Bold arrow** = Primary node (stores first)
        - ⇢ **Dashed arrows** = Replica nodes (receive copies)
        """)
        
    with st.expander("GET Operation Flow"):
        st.markdown("""
        **When you GET a key:**
        
        1. **Hash & Route**: Key is hashed to find its position on the ring
        2. **Find Primary**: The ring identifies the PRIMARY node
        3. **Read**: Primary node reads and returns the value
        4. **Consistency**: If using read repair (not shown), stale replicas get updated
        
        **Note**: In this simple implementation, GET reads from the primary node only.
        Real Cassandra uses configurable consistency levels (ONE, QUORUM, ALL).
        """)
    
    # Node data tables
    st.divider()
    st.subheader("Node Data")
    
    for node in st.session_state.nodes:
        with st.expander(f"Node {node.node_id} ({'🟢 Alive' if node.alive else '🔴 Dead'})"):
            if node.data:
                # Display data in a nice format
                for key, value in node.data.items():
                    if isinstance(value, dict):
                        if value.get('deleted'):
                            st.write(f"**{key}**: ~~DELETED~~ (timestamp: {value.get('timestamp', 'N/A')})")
                        else:
                            st.write(f"**{key}**: `{value.get('value', 'N/A')}` (timestamp: {value.get('timestamp', 'N/A')})")
                    else:
                        st.write(f"**{key}**: `{value}`")
            else:
                st.info("No data stored in this node.")
    
    # Network statistics
    st.divider()
    st.subheader("Network Statistics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Delivered Messages", st.session_state.network.stats.get("delivered_messages", 0))
    with col2:
        st.metric("Dropped Messages", st.session_state.network.stats.get("dropped_messages", 0))
    with col3:
        st.metric("Duplicated Messages", st.session_state.network.stats.get("duplicated_messages", 0))
    
    # Gossip state
    st.divider()
    st.subheader("Gossip Protocol State")
    
    # Display gossip state from each node in a more compact format
    for node in st.session_state.nodes:
        if hasattr(node, 'gossip'):
            gossip_state = node.gossip.get_membership_state()
            st.write(f"**Node {node.node_id}'s view:**")
            
            # Create a table-like display for better readability
            gossip_data = []
            for node_id, (version, state, timestamp) in gossip_state.items():
                status_emoji = "🟢" if state == "UP" else "🔴"
                gossip_data.append({
                    "Node": node_id,
                    "Status": f"{status_emoji} {state}",
                    "Version": version,
                    "Timestamp": f"{timestamp}ms"
                })
            
            if gossip_data:
                st.dataframe(gossip_data, use_container_width=True, hide_index=True)
            else:
                st.info("No gossip state available")
            
            st.write("")

if __name__ == "__main__":
    main()


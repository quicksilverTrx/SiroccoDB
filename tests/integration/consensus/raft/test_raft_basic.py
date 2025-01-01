import os
import sys
import unittest
import time
import logging
from typing import Dict, Optional

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
sys.path.insert(0, project_root)

from src.consensus.raft.node import RaftNode, RaftState

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
class TestRaftBasic(unittest.TestCase):
    def setUp(self):
        """Create and start a Raft cluster"""
        self.nodes = {}
        self.base_port = 6000
        self.num_nodes = 9
        
        logger.info("\n%s", "="*60)
        logger.info("Starting new test case")
        logger.info("="*60)
        
        logger.info(f"Creating {self.num_nodes} nodes starting at port {self.base_port}")
        
        logger.info("Setting up test Raft cluster...")
        
        # Create nodes
        for i in range(self.num_nodes):
            node_id = f"node{i}"
            node = RaftNode(node_id, "localhost", self.base_port + i)
            self.nodes[node_id] = node
            
        # Start nodes and add peers
        for node in self.nodes.values():
            node.start()
            time.sleep(0.5)  # Allow node to start
            
            # Add other nodes as peers
            for peer in self.nodes.values():
                if peer.node_id != node.node_id:
                    node.add_peer(peer.node_id, peer.host, peer.port)
                    
        time.sleep(2)  # Allow cluster to stabilize
        logger.info("Setup complete. Cluster initialized.")

    def tearDown(self):
        """Stop all nodes gracefully"""
        logger.info("\n=== Tearing down test case ===")
        
        logger.info("Tearing down test Raft cluster...")
        
        for node in reversed(list(self.nodes.values())):
            try:
                node.stop()
            except Exception as e:
                logger.error(f"Error stopping node {node.node_id}: {e}")
                
        time.sleep(1)  # Final cleanup
        logger.info("Teardown complete.")

    def get_leader(self) -> Optional[RaftNode]:
        """Helper to get current leader node"""
        leaders = [node for node in self.nodes.values() 
                  if node.raft_state == RaftState.LEADER]
        return leaders[0] if len(leaders) == 1 else None

    def wait_for_leader(self, timeout: float = 10.0) -> Optional[RaftNode]:
        """Wait for leader election with timeout"""
        logger.info(f"\nWaiting for leader election (timeout: {timeout}s)")
        start_time = time.time()
        while time.time() - start_time < timeout:
            leader = self.get_leader()
            if leader:
                logger.info(f"Leader elected: {leader.node_id}")
                return leader
            time.sleep(0.5)
        logger.warning("No leader elected within timeout!")
        return None

    def verify_cluster_stability(self, timeout=5):
        """Helper to verify cluster is in stable state"""
        logger.info("\nVerifying cluster stability (timeout=%ds)", timeout)
        start = time.time()
        while time.time() - start < timeout:
            leader = self.get_leader()
            if leader:
                followers = [n for n in self.nodes.values() 
                           if n.raft_state == RaftState.FOLLOWER]
                logger.info("Current state: leader=%s, followers=%d", 
                          leader.node_id, len(followers))
                if len(followers) == self.num_nodes - 1:
                    logger.info("Cluster stability verified ✓")
                    return True
            time.sleep(0.5)
        logger.warning("Cluster stability check failed ✗")
        return False

    def test_1_initial_leader_election(self):
        """Test that a single leader is elected"""
        logger.info("\n=== Starting initial leader election test ===")
        
        # Log initial state
        logger.info("Initial cluster state:")
        for node in self.nodes.values():
            logger.info(f"Node {node.node_id}: state={node.raft_state}, term={node.current_term}")
        
        leader = self.wait_for_leader()
        logger.info(f"Leader election result: {leader.node_id if leader else 'No leader'}")
        
        self.assertIsNotNone(leader, "Failed to elect a leader")
        self.assertTrue(self.verify_cluster_stability(), 
                       "Cluster failed to reach stable state")
        
        # Additional validation
        followers = [n for n in self.nodes.values() 
                    if n.raft_state == RaftState.FOLLOWER]
        self.assertEqual(len(followers), self.num_nodes - 1, 
                        "Incorrect number of followers")
        
        # Verify terms
        logger.info("\nVerifying leader election stability:")
        logger.info("Checking for consistent terms across nodes...")
        terms = {n.current_term for n in self.nodes.values()}
        logger.info(f"Terms found in cluster: {terms}")
        self.assertEqual(len(terms), 1, "Nodes have inconsistent terms")
        
        logger.info("Final cluster state:")
        for node in self.nodes.values():
            logger.info(f"Node {node.node_id}: state={node.raft_state}, term={node.current_term}")

    def test_2_basic_replication(self):
        """Test basic log replication"""
        logger.info("\n=== Starting basic replication test ===")
        
        leader = self.wait_for_leader()
        logger.info(f"Using leader {leader.node_id} for replication test")
        
        # Log initial state
        logger.info("Pre-write cluster state:")
        for node in self.nodes.values():
            logger.info(f"Node {node.node_id}: log size={len(node.log)}, commit_index={node.commit_index}")
        
        # Write data
        test_key, test_value = "test_key", "test_value"
        logger.info(f"Attempting write: key='{test_key}', value='{test_value}'")
        success = leader.write(test_key, test_value)
        logger.info(f"Write operation {'succeeded' if success else 'failed'}")
        
        self.assertTrue(self.verify_cluster_stability(), 
                       "Cluster not stable before replication test")
        
        logger.info("Testing basic log replication...")
        
        leader = self.wait_for_leader()
        self.assertIsNotNone(leader, "No leader available for test")
        
        # Write data
        test_key = "test_key"
        test_value = "test_value"
        success = leader.write(test_key, test_value)
        self.assertTrue(success, "Write operation failed")
        
        # Wait for replication
        time.sleep(2)
        
        # Verify replication
        logger.info("\nVerifying replication across nodes:")
        for node in self.nodes.values():
            value = node.read(test_key)
            logger.info(f"Node {node.node_id}: value='{value}'")
            self.assertEqual(value, test_value, f"Value not replicated to {node.node_id}")
        
        # Add commit index verification
        leader = self.get_leader()
        commit_index = leader.commit_index
        for node in self.nodes.values():
            self.assertEqual(node.commit_index, commit_index,
                           f"Inconsistent commit index in {node.node_id}")
        
        logger.info("Post-replication cluster state:")
        for node in self.nodes.values():
            value = node.read(test_key)
            logger.info(f"Node {node.node_id}: value='{value}', log size={len(node.log)}, commit_index={node.commit_index}")

    def test_3_leader_failure(self):
        """Test handling of leader failure"""
        logger.info("\n=== Starting leader failure test ===")
        
        initial_leader = self.wait_for_leader()
        logger.info(f"Initial leader: {initial_leader.node_id}, term={initial_leader.current_term}")
        logger.info("Cluster state before leader failure:")
        for node in self.nodes.values():
            logger.info(f"Node {node.node_id}: state={node.raft_state}, term={node.current_term}")
        
        self.assertTrue(self.verify_cluster_stability(),
                       "Cluster not stable before leader failure test")
        
        logger.info("Testing leader failure handling...")
        
        initial_leader = self.wait_for_leader()
        self.assertIsNotNone(initial_leader, "No initial leader")
        initial_term = initial_leader.current_term
        
        # Stop leader
        logger.info(f"\nStopping initial leader: {initial_leader.node_id}")
        initial_leader.stop()
        logger.info("Waiting for new leader election...")
        
        # Wait for new leader
        time.sleep(5)
        new_leader = self.get_leader()
        
        self.assertIsNotNone(new_leader, "Failed to elect new leader")
        self.assertNotEqual(new_leader.node_id, initial_leader.node_id)
        self.assertGreater(new_leader.current_term, initial_term)
        
        # Add post-recovery stability check
        self.assertTrue(self.verify_cluster_stability(),
                       "Cluster failed to stabilize after leader failure")
        
        # Verify no split votes
        candidates = [n for n in self.nodes.values() 
                     if n.raft_state == RaftState.CANDIDATE]
        self.assertEqual(len(candidates), 0, 
                        "Split vote detected after recovery")
        
        logger.info(f"New leader after recovery: {new_leader.node_id if new_leader else 'None'}")
        logger.info("Final cluster state:")
        for node in self.nodes.values():
            logger.info(f"Node {node.node_id}: state={node.raft_state}, term={node.current_term}")

    def test_5_network_partition(self):
        """Test handling of network partition"""
        logger.info("Testing network partition...")
        
        leader = self.wait_for_leader()
        self.assertIsNotNone(leader, "No leader available for test")
        
        # Split cluster
        partition1 = {leader.node_id}
        partition2 = set(self.nodes.keys()) - partition1
        
        # Simulate partition
        for node in self.nodes.values():
            if node.node_id in partition1:
                node._peers = [(p[0], p[1], p[2]) for p in node._peers
                             if p[0] in partition1]
            else:
                node._peers = [(p[0], p[1], p[2]) for p in node._peers
                             if p[0] in partition2]
        
        time.sleep(5)
        
        # Heal partition
        # Restore connections
        for node in self.nodes.values():
            node._peers = [(p.node_id, p.host, p.port) 
                          for p in self.nodes.values()
                          if p.node_id != node.node_id]
        
        time.sleep(5)
        
        # Verify cluster convergence
        final_leader = self.wait_for_leader()
        self.assertIsNotNone(final_leader, "Failed to re-elect leader after partition heal")

    def test_4_term_voting(self):
        """Test term increments and voting behavior"""
        logger.info("Testing term and voting behavior...")
        
        # Verify initial term numbers
        initial_terms = {node.node_id: node.current_term for node in self.nodes.values()}
        
        # Stop current leader to force election
        leader = self.wait_for_leader()
        self.assertIsNotNone(leader)
        leader.stop()
        
        # Wait for new election
        time.sleep(5)
        
        # Verify term increased
        new_terms = {node.node_id: node.current_term for node in self.nodes.values()}
        for node_id, term in new_terms.items():
            if node_id != leader.node_id:  # Exclude stopped leader
                self.assertGreater(term, initial_terms[node_id], 
                                 f"Term should increase for {node_id}")

    def test_6_log_consistency(self):
        """Test log consistency and conflict resolution"""
        self.assertTrue(self.verify_cluster_stability(),
                       "Cluster not stable before log consistency test")
        
        logger.info("Testing log consistency...")
        
        leader = self.wait_for_leader()
        self.assertIsNotNone(leader)
        
        # Write multiple entries
        for i in range(5):
            success = leader.write(f"key{i}", f"value{i}")
            self.assertTrue(success)
        
        time.sleep(2)  # Allow replication
        
        # Verify logs are consistent
        reference_log = self.nodes[leader.node_id].log
        for node in self.nodes.values():
            self.assertEqual(len(node.log), len(reference_log),
                           f"Log length mismatch for {node.node_id}")
            for i, entry in enumerate(node.log):
                self.assertEqual(entry['term'], reference_log[i]['term'],
                               f"Term mismatch at index {i} for {node.node_id}")
                self.assertEqual(entry['command'], reference_log[i]['command'],
                               f"Command mismatch at index {i} for {node.node_id}")
        
        # Verify log properties
        for node in self.nodes.values():
            # Verify terms are non-decreasing
            for i in range(1, len(node.log)):
                self.assertGreaterEqual(
                    node.log[i]['term'], 
                    node.log[i-1]['term'],
                    f"Term regression in {node.node_id}'s log"
                )

    def test_7_commit_safety(self):
        """Test commit safety and majority consensus"""
        # Add pre-test stability verification
        self.assertTrue(self.verify_cluster_stability(),
                       "Cluster not stable before commit safety test")
        
        logger.info("Testing commit safety...")
        
        leader = self.wait_for_leader()
        self.assertIsNotNone(leader)
        
        # Write value but stop leader before replication
        key, value = "test_key", "test_value"
        leader.write(key, value)
        leader.stop()
        
        # Wait for new leader
        time.sleep(5)
        new_leader = self.wait_for_leader()
        self.assertIsNotNone(new_leader)
        
        # Verify committed entries are preserved
        committed_value = new_leader.read(key)
        if committed_value is not None:
            self.assertEqual(committed_value, value, 
                           "Committed value should be preserved")
            
        # Verify all nodes have consistent committed entries
        for node in self.nodes.values():
            if node.node_id != leader.node_id:  # Exclude stopped leader
                node_value = node.read(key)
                if committed_value is not None:
                    self.assertEqual(node_value, committed_value,
                                   f"Inconsistent committed value in {node.node_id}")
        
        # Add majority consensus verification
        if committed_value is not None:
            matching_nodes = sum(1 for n in self.nodes.values() 
                               if n.read(key) == value)
            self.assertGreater(matching_nodes, self.num_nodes // 2,
                             "Committed value not present in majority")

    def test_8_stale_leader(self):
        """Test handling of stale leader"""
        # Add pre-test stability check
        self.assertTrue(self.verify_cluster_stability(),
                       "Cluster not stable before stale leader test")
        
        logger.info("Testing stale leader handling...")
        
        leader = self.wait_for_leader()
        self.assertIsNotNone(leader)
        initial_term = leader.current_term
        
        # Partition leader
        leader._peers = []  # Isolate leader
        
        # Wait for new leader in majority partition
        time.sleep(5)
        new_leader = None
        for node in self.nodes.values():
            if node.node_id != leader.node_id and node.raft_state == RaftState.LEADER:
                new_leader = node
                break
                
        self.assertIsNotNone(new_leader, "New leader should be elected")
        self.assertGreater(new_leader.current_term, initial_term)
        
        # Verify stale leader can't commit
        stale_write = leader.write("stale_key", "stale_value")
        self.assertFalse(stale_write, "Stale leader should not commit")
        
        # Heal partition
        leader._peers = [(p.node_id, p.host, p.port) 
                        for p in self.nodes.values()
                        if p.node_id != leader.node_id]
                        
        time.sleep(2)
        
        # Verify stale leader steps down
        self.assertEqual(leader.raft_state, RaftState.FOLLOWER,
                        "Stale leader should step down")
        self.assertGreaterEqual(leader.current_term, new_leader.current_term,
                              "Stale leader should update term")
        
        # Add post-healing verification
        self.assertTrue(self.verify_cluster_stability(),
                       "Cluster failed to stabilize after partition heal")
        
        # Verify log consistency after healing
        leader = self.get_leader()
        reference_log = leader.log
        for node in self.nodes.values():
            self.assertEqual(node.log, reference_log,
                           f"Log inconsistency after partition heal in {node.node_id}")

    def test_9_leader_election_safety(self):
        """Verify that at most one leader can be elected per term"""
        logger.info("Testing leader election safety...")
        
        # Get initial leader and term
        leader = self.wait_for_leader()
        self.assertIsNotNone(leader)
        initial_term = leader.current_term
        
        # Track all terms and leaders seen
        term_leaders = {}  # term -> set of leaders
        
        # Force multiple elections by stopping and starting nodes
        for i in range(3):
            # Stop current leader
            current_leader = self.get_leader()
            if current_leader:
                current_leader.stop()
            time.sleep(2)
            
            # Wait for new leader
            new_leader = self.wait_for_leader()
            if new_leader:
                term = new_leader.current_term
                if term not in term_leaders:
                    term_leaders[term] = set()
                term_leaders[term].add(new_leader.node_id)
        
        # Verify at most one leader per term
        for term, leaders in term_leaders.items():
            self.assertEqual(len(leaders), 1,
                               f"Multiple leaders found in term {term}: {leaders}")

    def test_10_write_linearizability(self):
        """Verify that writes are linearizable"""
        logger.info("\n=== Starting write linearizability test ===")
        
        leader = self.wait_for_leader()
        logger.info(f"Using leader {leader.node_id} for linearizability test")
        
        # Log initial state
        logger.info("Pre-test cluster state:")
        for node in self.nodes.values():
            logger.info(f"Node {node.node_id}: log size={len(node.log)}, commit_index={node.commit_index}")
            
        # Track writes with timing
        writes = []
        for i in range(5):
            key = f"key{i}"
            value = f"value{i}"
            start_time = time.time()
            logger.info(f"Attempting write #{i+1}: key='{key}', value='{value}'")
            success = leader.write(key, value)
            end_time = time.time()
            writes.append((start_time, end_time, key, value))
            logger.info(f"Write #{i+1} {'succeeded' if success else 'failed'} in {end_time - start_time:.3f}s")
        
        logger.info("Verifying read consistency across nodes...")
        for node in self.nodes.values():
            logger.info(f"Checking reads on {node.node_id}:")
            for _, _, key, expected_value in writes:
                current_value = node.read(key)
                logger.info(f"  {key}: expected='{expected_value}', actual='{current_value}'")
                self.assertEqual(current_value, expected_value)
                
        logger.info("Final cluster state:")
        for node in self.nodes.values():
            logger.info(f"Node {node.node_id}: log size={len(node.log)}, commit_index={node.commit_index}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
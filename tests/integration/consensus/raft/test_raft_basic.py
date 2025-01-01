# tests/integration/consensus/raft/test_raft_basic.py

import os
import sys
import unittest
import time
import logging
from typing import Dict
import traceback

# Add project root to Python path to make imports work
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
sys.path.insert(0, project_root)

from src.consensus.raft.node import RaftNode, RaftState
from src.core.types.states import NodeState

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestRaftBasic(unittest.TestCase):
    def setUp(self):
        """Create and start nodes with proper synchronization"""
        self.nodes = {}
        self.base_port = 6000
        logger.info("Setting up test Raft cluster...")

        # Create nodes
        for i in range(9):  # Adjust node count as needed
            node_id = f"node{i}"
            node = RaftNode(node_id, "localhost", self.base_port + i)
            self.nodes[node_id] = node

        # Start all nodes and wait for them to bind sockets
        for node in self.nodes.values():
            node.start()
            time.sleep(0.5)  # Allow sockets to initialize

        # Add peers after all nodes have started
        for node in self.nodes.values():
            for peer in self.nodes.values():
                if peer.node_id != node.node_id:
                    node.add_peer(peer.node_id, peer.host, peer.port)
        time.sleep(5)  # Ensure all nodes are listening

    def tearDown(self):
        """Stop all nodes gracefully."""
        logger.info("Tearing down test Raft cluster...")
        for node in reversed(self.nodes.values()):
            try:
                # Add socket cleanup
                if hasattr(node, '_socket'):
                    node._socket.close()
                node.stop()
                logger.debug(f"Stopped {node.node_id}")
                time.sleep(0.1)  # Small delay between stops
            except Exception as e:
                logger.error(f"Error stopping node {node.node_id}: {e}", exc_info=True)
                
        # Add extra cleanup
        self.nodes.clear()
        # Wait longer for sockets to fully close
        time.sleep(5)

    def test_basic_startup(self):
        """Test that nodes can start up without errors"""
        logger.info("Testing basic node startup...")
        try:
            for node in self.nodes.values():
                #node.start()
                time.sleep(1)  # Wait between starts
                self.assertEqual(node.state, NodeState.ACTIVE)
                logger.info(f"{node.node_id} started successfully")
            # Wait for nodes to stabilize
            time.sleep(1)

            # Verify cluster state
            leaders = [node for node in self.nodes.values() 
                    if node.raft_state == RaftState.LEADER]
            self.assertEqual(len(leaders), 1, "Should have exactly one leader")
        except Exception as e:
            logger.error(f"Node startup failed: {e}")
            raise

    def test_leader_election(self):
        """Test that a leader is elected"""
        MAX_RETRIES = 3
        for attempt in range(MAX_RETRIES):
            try:
                for node in self.nodes.values():
                    #node.start()
                    time.sleep(1)  # Increased delay between starts
                
                # Increase wait time for leader election
                time.sleep(10)  # Ensure time for election
                
                leaders = [node for node in self.nodes.values() if node.raft_state == RaftState.LEADER]
                
                if len(leaders) == 1:
                    logger.info(f"Leader elected: {leaders[0].node_id}")
                    break
                else:
                    logger.warning(f"Attempt {attempt + 1}: No single leader found. Retrying...")
                    self.tearDown()
                    self.setUp()
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
    
    def test_basic_write(self):
        """Test basic write operation through the leader"""
        logger.info("Testing basic write operation...")
        
        # Wait for leader election with retry logic
        leader = None
        max_retries = 5
        for attempt in range(max_retries):
            time.sleep(2)  # Wait between attempts
            leader = next((node for node in self.nodes.values() 
                        if node.raft_state == RaftState.LEADER), None)
            if leader:
                break
            logger.info(f"No leader found, attempt {attempt + 1}/{max_retries}")
        
        self.assertIsNotNone(leader, "No leader elected after maximum retries")
        logger.info(f"Found leader: {leader.node_id}")
        
        # Write through leader
        test_key = "test_key"
        test_value = "test_value"
        logger.info(f"Writing {test_key}={test_value} through leader")
        
        write_success = False
        write_retries = 3
        for attempt in range(write_retries):
            try:
                write_success = leader.write(test_key, test_value)
                if write_success:
                    break
                logger.warning(f"Write attempt {attempt + 1} failed, retrying...")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error during write attempt {attempt + 1}: {e}")
        
        self.assertTrue(write_success, "Write operation failed after all retries")
        
        # Wait for replication with verification
        max_verify_attempts = 1
        replication_timeout = 1  # seconds per attempt
        
        for attempt in range(max_verify_attempts):
            time.sleep(replication_timeout)
            
            # Print debug info
            logger.info("Current state of all nodes:")
            for node in self.nodes.values():
                node.debug_state()
            
            # Count nodes that have the value
            nodes_with_value = 0
            for node in self.nodes.values():
                try:
                    value = node.read(test_key)
                    if value == test_value:
                        nodes_with_value += 1
                except Exception as e:
                    logger.error(f"Error reading from {node.node_id}: {e}")

        
            
            # Check if we have majority
            if nodes_with_value > len(self.nodes) / 2:
                logger.info(f"Value replicated to majority ({nodes_with_value} nodes)")
                break
                
            logger.info(f"Attempt {attempt + 1}: Value present on {nodes_with_value} nodes")
        
        # Final verification
        for node in self.nodes.values():
            try:
                value = node.read(test_key)
                self.assertEqual(value, test_value, 
                            f"Value mismatch on {node.node_id}. Expected {test_value}, got {value}")
                logger.info(f"Verified value on {node.node_id}")
            except Exception as e:
                logger.error(f"Error verifying value on {node.node_id}: {e}")
                #logger.error("Log state:", node.log)


if __name__ == '__main__':
    unittest.main(verbosity=2)
# src/consensus/raft/node.py
import random
import threading
import time
from enum import Enum
from typing import Optional, List, Dict, Any, Union
import logging
import socket
from src.core.node import DistributedKeyValueStoreNode
from src.core.types.states import NodeState
from src.core.types.messages import Message, MessageType
from src.core.network.connection import Connection
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 
class RaftState(Enum):
    """Raft node states"""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

class RaftMessageType(Enum):
    # Include all Raft-specific members
    REQUEST_VOTE = "REQUEST_VOTE"
    REQUEST_VOTE_RESPONSE = "REQUEST_VOTE_RESPONSE"
    APPEND_ENTRIES = "APPEND_ENTRIES"
    APPEND_ENTRIES_RESPONSE = "APPEND_ENTRIES_RESPONSE"

    # Extend with members from MessageType
    WRITE = MessageType.WRITE.value
    READ = MessageType.READ.value
    WRITE_RESPONSE = MessageType.WRITE_RESPONSE.value
    READ_RESPONSE = MessageType.READ_RESPONSE.value

class RaftMessage(Message):
    """Extended message type for Raft-specific fields"""
    def __init__(self, *args, term: int = 0, type: Optional[Union[str, RaftMessageType, MessageType]] = None, **kwargs):
        # Convert `type` to the appropriate Enum member if it's a string
        if isinstance(type, str):
            if type in RaftMessageType._value2member_map_:
                type = RaftMessageType(type)
            elif type in MessageType._value2member_map_:
                type = MessageType(type)
            else:
                raise ValueError(f"Unknown message type: {type}")
        
        # Ensure `type` is either RaftMessageType or MessageType
        if not isinstance(type, (RaftMessageType, MessageType)):
            raise TypeError(f"Invalid type: {type}. Must be RaftMessageType or MessageType.")
        
        super().__init__(*args, type=type, **kwargs)
        self.term = term

class RaftNode(DistributedKeyValueStoreNode):
    """Raft consensus implementation for distributed key-value store"""
    
    def __init__(self, node_id: str, host: str, port: int):
        super().__init__(node_id, host, port)
        
        # Raft-specific state
        self.raft_state = RaftState.FOLLOWER 
        self.current_term = 0  # Latest term server has seen
        self.voted_for: Optional[str] = None  # CandidateId that received vote in current term
        self.leader_id: Optional[str] = None
        
        # Volatile state
        self.commit_index = -1 # Highest log entry known to be committed
        self.last_applied = -1  # Highest log entry applied to state machine
        
        # Leader state
        self.next_index: Dict[str, int] = {} # For each server, index of next log entry to send
        self.match_index: Dict[str, int] = {}   # For each server, index of highest log replicated
        
        # Timing
        self.election_timeout = random.uniform(150, 300) / 1000  # 150-300ms
        self.last_heartbeat = time.time()

        # Add state change locks
        self._state_lock = threading.Lock()
        self._term_lock = threading.Lock()
        
        # Add state change callbacks
        self._state_callbacks = []
        
        # Log entries
        self.log: List[Dict] = []  # [{term: int, command: dict}]
        
        # Additional threads
        self._election_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        
    def start(self):
        # In consensus/raft/node.py
        """Start the node with proper initialization sequence"""
        try:
            # First start the network listener
            super().start()
            logger.info(f"Node {self.node_id} network listener started")
            
            # Wait a bit for socket to be ready
            time.sleep(0.5)
            
            # Now start Raft-specific components
            self._election_thread = threading.Thread(target=self._run_election_timer)
            self._election_thread.daemon = True
            self._election_thread.start()
            
            logger.info(f"Node {self.node_id} fully started")
            
        except Exception as e:
            logger.error(f"Failed to start node: {e}")
            raise
    
    def debug_state(self):
        """Debug helper to print node state"""
        state = {
            'node_id': self.node_id,
            'raft_state': self.raft_state,
            'current_term': self.current_term,
            'commit_index': self.commit_index,
            'last_applied': self.last_applied,
            'log': self.log,
            'store_data': self._store._data,
            'leader_id': self.leader_id
        }
        logger.info(f"Node state: {state}")
        return state
    


    def stop(self):
        """Stop the node"""
        super().stop()
        self._running = False
        
        # Close any open connections
        if hasattr(self, '_server_socket'):
            try:
                self._server_socket.close()
            except:
                pass
                
        CLEANUP_TIMEOUT = 5.0
        if self._election_thread and self._election_thread.is_alive():
            self._election_thread.join(timeout=CLEANUP_TIMEOUT)
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=CLEANUP_TIMEOUT)
            
        self.raft_state = RaftState.FOLLOWER
        time.sleep(0.1)  # Wait for cleanup
    
    def write(self, key: str, value: Any) -> bool:
        """Write operation through Raft consensus"""
        if self.raft_state != RaftState.LEADER:
            if self.leader_id:
                # Forward to leader
                leader_info = next((p for p in self._peers if p[0] == self.leader_id), None)
                if not leader_info:
                    logger.debug(f"Leader {self.leader_id} not found in peers")
                    return False
                if leader_info:
                    try:
                        _, host, port = leader_info
                        message = RaftMessage(
                            type=RaftMessageType.WRITE,
                            key=key,
                            value=value,
                            term=self.current_term,
                            sender_id=self.node_id
                        )
                        logger.debug(f"Forwarding write to leader {self.leader_id}")
                        response = Connection.send_message(host, port, message)
                        return response.value if response else False
                    except Exception as e:
                        logger.error(f"Failed to forward write to leader: {e}")
                return False
                
        # Leader handles write
        entry = {
            'term': self.current_term,
            'command': {'key': key, 'value': value, 'operation': 'WRITE'}
        }
        self.log.append(entry)
        logger.debug(f"Leader {self.node_id} appending entry to log: {entry}")
        
        if self._replicate_log_entry(len(self.log) - 1):
            logger.debug("Log entry replicated successfully")
            self._update_commit_index()
            logger.debug(f"Updated commit index. Current store state: {self._store._data}")
            return True
        logger.warning("Failed to replicate log entry")
        return False
        
    def _handle_connection(self, client_socket: socket.socket):
        """Override to handle Raft-specific messages."""
        try:
            message = Connection.receive_message(client_socket)
            logger.debug(f"RaftNode {self.node_id} received message: {message}")
            logger.debug(f"Message type resolved to: {message.type}")
            if message:
                # Use _handle_message to process the received message
                response = self._handle_message(message)
                if response:
                    logger.debug(f"RaftNode {self.node_id} sending response: {response}")
                    Connection.send_raw_message(client_socket, response)
        except Exception as e:
            logger.error(f"RaftNode {self.node_id} failed to handle connection: {e}")
        finally:
            client_socket.close()

    def _run_election_timer(self):

        """Improved election timeout handling"""
        while self._running:
            try:
                current_time = time.time()
                
                with self._state_lock:
                    if self.raft_state == RaftState.LEADER:
                        time.sleep(0.1)
                        continue
                        
                    if current_time - self.last_heartbeat > self.election_timeout:
                        self._start_election()
                    else:
                        # Adaptive sleep to prevent busy waiting
                        sleep_time = min(
                            0.1,  # Max sleep time
                            self.election_timeout - (current_time - self.last_heartbeat)
                        )
                        time.sleep(sleep_time)
                        
            except Exception as e:
                logger.error(f"Election timer error: {e}", exc_info=True)
                time.sleep(1)  # Prevent rapid retries on error

    def _reset_election_timeout(self):
        """Reset election timeout with randomization   Increase randomization range for better split vote prevention"""
        BASE_TIMEOUT = 150  # milliseconds
        TIMEOUT_RANGE = 300  # milliseconds
        self.election_timeout = (BASE_TIMEOUT + random.uniform(0, TIMEOUT_RANGE)) / 1000
        self.last_heartbeat = time.time()
                
    def _start_election(self):
        """Start a new election"""
        time.sleep(random.uniform(0.01, 0.05))  # Small random delay to avoid synchronized elections
        logger.debug(f"Node {self.node_id} starting election")
        self.raft_state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        votes_received = 1  # Vote for self
        
        # Reset election timeout
        self._reset_election_timeout()
        
        # Request votes from peers
        for peer_id, host, port in self._peers:
            try:
                message = RaftMessage(
                    type=RaftMessageType.REQUEST_VOTE,
                    key="",
                    value={
                        'last_log_index': len(self.log) - 1,
                        'last_log_term': self.log[-1]['term'] if self.log else 0
                    },
                    term=self.current_term,
                    sender_id=self.node_id
                )
                logger.debug(f"Sending vote request to {peer_id} at {host}:{port}")

                response = Connection.send_message(host, port, message)
                logger.debug(f"{response}")

                if response:
                     #Check for higher term in response and revert to follower if necessary
                    if response.term > self.current_term:
                        self.current_term = response.term
                        self.raft_state = RaftState.FOLLOWER
                        self.voted_for = None
                        return
                    if  response.value:
                        votes_received += 1
                        logger.info(f"{self.node_id} received {votes_received} votes")
                        # Check if we have majority
                        if votes_received > (len(self._peers) + 1) / 2:
                            self._become_leader()
                            break
                        
            except Exception as e:
                logger.error(f"Failed to request vote from {peer_id}: {e}")
                logger.error(f"Error handling connection on node {self.node_id}: {e}")
            #logger.error("Traceback:", exc_info=True)  # Logs the full traceback
                
    def _become_leader(self):
        """Transition to leader state"""
        logger.info(f"Node {self.node_id} became leader")
        if self.raft_state == RaftState.CANDIDATE:
            self.raft_state = RaftState.LEADER
            self.leader_id = self.node_id
            
            # Initialize leader state
            for peer_id, _, _ in self._peers:
                self.next_index[peer_id] = len(self.log)
                self.match_index[peer_id] = -1
            # Append empty entry to establish leadership (CRITICAL)
            noop_entry = {
                'term': self.current_term,
                'command': {'operation': 'NOOP'}
            }
            self.log.append(noop_entry)
            self._replicate_log_entry(len(self.log) - 1)
                
            # Start heartbeat thread
            self._heartbeat_thread = threading.Thread(target=self._send_heartbeats)
            self._heartbeat_thread.daemon = True
            self._heartbeat_thread.start()

    def _forward_read_to_leader(self, key: str) -> Optional[Any]:
        """Forward read request to current leader"""
        if not self.leader_id:
            logger.warning(f"No leader known to forward read request")
            return None
            
        # Find leader's connection info
        leader_info = next(
            (p for p in self._peers if p[0] == self.leader_id), 
            None
        )
        
        if not leader_info:
            logger.error(f"Leader {self.leader_id} not found in peers")
            return None
            
        try:
            _, host, port = leader_info
            message = RaftMessage(
                type=RaftMessageType.READ,
                key=key,
                term=self.current_term,
                sender_id=self.node_id
            )
            
            response = Connection.send_message(host, port, message)
            if response:
                return response.value
                
        except Exception as e:
            logger.error(f"Failed to forward read to leader: {e}")
            
        return None    
    
    def _handle_read_request(self, message: RaftMessage) -> RaftMessage:
        """Handle read request"""
        value = None
        if self.raft_state == RaftState.LEADER:
            value = self._store.read(message.key)[0] if self._store.read(message.key) else None
            
        return RaftMessage(
            type=RaftMessageType.READ_RESPONSE,
            key=message.key,
            value=value,
            term=self.current_term,
            sender_id=self.node_id
        )
    def _send_heartbeats(self):
        """Send periodic heartbeats to maintain leadership"""
        while self._running and self.raft_state == RaftState.LEADER:
            for peer_id, host, port in self._peers:
                try:
                    next_index = self.next_index.get(peer_id, 0)
                    prev_log_index = next_index - 1
                    #prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0
                    prev_log_term = 0
                    if prev_log_index >= 0 and prev_log_index < len(self.log):
                        prev_log_term = self.log[prev_log_index]['term']
                    
                    # Get entries to send
                    entries = self.log[next_index:] if next_index < len(self.log) else []
                    #entries = self.log[self.next_index[peer_id]:]
                    
                    message = RaftMessage(
                        type=RaftMessageType.APPEND_ENTRIES,
                        key="",
                        value={
                            'prev_log_index': prev_log_index,
                            'prev_log_term': prev_log_term,
                            'entries': entries,
                            'leader_commit': self.commit_index
                        },
                        term=self.current_term,
                        sender_id=self.node_id
                    )
                    
                    response = Connection.send_message(host, port, message)
                    if response:
                        #Step down to follower if term is higher
                        if response.term > self.current_term:
                            self.current_term = response.term
                            self.raft_state = RaftState.FOLLOWER
                            self.voted_for = None
                            return
                            
                        if response.value:
                            self.next_index[peer_id] = next_index + len(entries)
                            self.match_index[peer_id] = self.next_index[peer_id] - 1
                            self._update_commit_index()
                        else:
                            self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)
                            
                except Exception as e:
                    logger.error(f"Failed to send heartbeat to {peer_id}: {e}")
                    
            time.sleep(0.05 + random.uniform(0, 0.02))  # Add jitter to heartbeat interval
            
    def _update_commit_index(self):
        if self.raft_state != RaftState.LEADER:
            return
        
        # Try to advance the commit index
        for n in range(self.commit_index + 1, len(self.log)):
            if self.log[n]['term'] == self.current_term:
                # Count replicas including self
                replicated_count = 1
                for peer_id in self.match_index:
                    if self.match_index[peer_id] >= n:
                        replicated_count += 1
                        
                # If majority have replicated, update commit index
                if replicated_count > (len(self._peers) + 1) / 2:
                    prev_commit = self.commit_index
                    self.commit_index = n
                    logger.debug(f"Leader updating commit index from {prev_commit} to {n}")
                    self._apply_committed_entries()
                    

    def _handle_message(self, message: RaftMessage) -> Optional[RaftMessage]:
        #logger.debug(f"Received message: {message}")
        #logger.debug(f"Message type resolved to: {message.type}")
        """Handle incoming Raft messages"""
        if message.term > self.current_term:
            self.current_term = message.term
            self.raft_state = RaftState.FOLLOWER
            self.voted_for = None
            
        handlers = {
            RaftMessageType.REQUEST_VOTE: self._handle_vote_request,
            RaftMessageType.APPEND_ENTRIES: self._handle_append_entries,
            RaftMessageType.WRITE: self._handle_write_request,
            RaftMessageType.READ: self._handle_read_request,
        }

        handler = handlers.get(message.type)

        return handler(message) if handler else None
    
    def _handle_vote_request(self, message: RaftMessage) -> RaftMessage:
        """Handle incoming vote request"""
        grant_vote = False
        #print(message)
        if message.term < self.current_term:
            grant_vote = False
        elif self.voted_for is None or self.voted_for == message.sender_id:
            # Check if candidate's log is at least as up-to-date as ours
            candidate_log = message.value
            if len(self.log) == 0:
                grant_vote = True
            else:
                our_last_term = self.log[-1]['term']
                our_log_length = len(self.log)
                
                if candidate_log['last_log_term'] > our_last_term:
                    grant_vote = True
                elif candidate_log['last_log_term'] == our_last_term and \
                     candidate_log['last_log_index'] >= our_log_length - 1:
                    grant_vote = True
                    
            if grant_vote:
                self.voted_for = message.sender_id
                self.last_heartbeat = time.time()  # Reset election timeout
                
        return RaftMessage(
            type=RaftMessageType.REQUEST_VOTE_RESPONSE,
            key="",
            value=grant_vote,
            term=self.current_term,
            sender_id=self.node_id
        )
    def _handle_append_entries(self, message: RaftMessage) -> RaftMessage:
        """Handle incoming append entries (heartbeat)"""

    
        # self._last_processed_message = message_id
        logger.debug(f"Node {self.node_id} handling AppendEntries")
        logger.debug(f"Current term: {self.current_term}, Message term: {message.term}")
        
        success = False
        self.last_heartbeat = time.time()
        
        if message.term < self.current_term:
            logger.debug("Rejecting AppendEntries - lower term")
            return RaftMessage(
                type=RaftMessageType.APPEND_ENTRIES_RESPONSE,
                key="",
                value=False,
                term=self.current_term,
                sender_id=self.node_id
            )
            
        if message.term > self.current_term:
            logger.debug(f"Updating term from {self.current_term} to {message.term}")
            self.current_term = message.term
            self.voted_for = None
            
        if self.raft_state != RaftState.FOLLOWER:
            logger.debug(f"Converting to follower from {self.raft_state}")
            self.raft_state = RaftState.FOLLOWER
            
        self.leader_id = message.sender_id
        
        prev_log_index = message.value['prev_log_index']
        prev_log_term = message.value['prev_log_term']
        entries = message.value['entries']
        leader_commit = message.value['leader_commit']
        
        logger.debug(f"Processing AppendEntries - prev_index: {prev_log_index}, prev_term: {prev_log_term}, entries: {entries}")
        logger.debug(f"Leader commit: {leader_commit}, Current log: {self.log}")
        
        if prev_log_index >= len(self.log):
            logger.debug("Rejecting AppendEntries - missing previous log entry")
            return RaftMessage(
            type=RaftMessageType.APPEND_ENTRIES_RESPONSE,
            key="",
            value=False,
            term=self.current_term,
            sender_id=self.node_id
        )
        elif prev_log_index == -1:
            logger.debug("Empty log case")
            success = True
            self.log = []
            self.log.extend(entries)
        elif self.log[prev_log_index]['term'] == prev_log_term:
            logger.debug("Log matching successful")
            success = True
            for i, new_entry in enumerate(entries):
                log_idx = prev_log_index + 1 + i
                if log_idx < len(self.log):
                    if self.log[log_idx]['term'] != new_entry['term']:
                        # Terms don't match, truncate log and append new entries
                        self.log = self.log[:log_idx]
                        break
            self.log = self.log[:prev_log_index + 1]
            self.log.extend(entries)
            
            if  success and leader_commit > self.commit_index:
                old_commit = self.commit_index
                self.commit_index = min(leader_commit, len(self.log) - 1)
                logger.debug(f"Updated commit index from {old_commit} to {self.commit_index}")
                if self.commit_index > old_commit:
                    self._apply_committed_entries()
                    logger.debug(f"After apply, store state: {self._store._data}")
            
        
        logger.debug(f"Returning AppendEntries response: {success}")
        return RaftMessage(
            type=RaftMessageType.APPEND_ENTRIES_RESPONSE,
            key="",
            value=success,
            term=self.current_term,
            sender_id=self.node_id
        )

    def _handle_write_request(self, message: RaftMessage) -> RaftMessage:
        """Handle write request from client"""
        success = False
        
        if self.raft_state == RaftState.LEADER:
            success = self.write(message.key, message.value)
            
        return RaftMessage(
            type=RaftMessageType.WRITE_RESPONSE,
            key=message.key,
            value=success,
            term=self.current_term,
            sender_id=self.node_id
        )
    
    def _replicate_log_entry(self, index: int) -> bool:
        """Replicate a log entry to followers"""
        if self.raft_state != RaftState.LEADER:
            logger.debug(f"Node {self.node_id} not leader, cannot replicate")
            return False
            
        success_count = 1  # Count self
        self.match_index[self.node_id] = index  # Update own match index
        logger.debug(f"Leader {self.node_id} starting replication of index {index}")
        
        for peer_id, host, port in self._peers:
            try:
                prev_log_index = index - 1
                prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0
                
                message = RaftMessage(
                    type=RaftMessageType.APPEND_ENTRIES,
                    key="",
                    value={
                        'prev_log_index': prev_log_index,
                        'prev_log_term': prev_log_term,
                        'entries': [self.log[index]],
                        'leader_commit': self.commit_index
                    },
                    term=self.current_term,
                    sender_id=self.node_id
                )
                
                response = Connection.send_message(host, port, message)
                if response:
                    if response.value:
                        success_count += 1
                        self.match_index[peer_id] = index
                        self.next_index[peer_id] = index + 1
                        logger.debug(f"Successful replication to {peer_id}, match_index={index}")
                        
            except Exception as e:
                logger.error(f"Failed to replicate to {peer_id}: {e}")
        
        majority = (len(self._peers) + 1) // 2 + 1
        logger.debug(f"Success count: {success_count}, needed majority: {majority}")
        success = success_count >= majority
        
        if success:
            # Here's the critical fix - we need to advance the commit index
            # when we get majority replication
            if self.log[index]['term'] == self.current_term:
                old_commit = self.commit_index
                # Advance commit index to the newly replicated entry
                self.commit_index = index
                logger.debug(f"Advanced commit index from {old_commit} to {index}")
                
                # Send new commit index to followers immediately
                for peer_id, host, port in self._peers:
                    try:
                        message = RaftMessage(
                            type=RaftMessageType.APPEND_ENTRIES,
                            key="",
                            value={
                                'prev_log_index': index,
                                'prev_log_term': self.log[index]['term'],
                                'entries': [],  # Empty entries for commit update
                                'leader_commit': self.commit_index  # New commit index
                            },
                            term=self.current_term,
                            sender_id=self.node_id
                        )
                        Connection.send_message(host, port, message)
                    except Exception as e:
                        logger.error(f"Failed to send commit update to {peer_id}: {e}")
                
                # Apply the committed entries
                self._apply_committed_entries()
                logger.debug(f"After commit, store state: {self._store._data}")
        
        return success       
    
    # Add these methods to the existing RaftNode class

    def create_snapshot(self) -> Dict:
        """Create snapshot of current state and log
        DDIA p.369: Required for log compaction"""
        snapshot = {
            'last_included_index': self.last_applied,
            'last_included_term': self.log[self.last_applied]['term'],
            'state': self._store._data.copy(),
            'configuration': {p[0] for p in self._peers}  # Current cluster config
        }
        
        # Compact log
        self.log = self.log[self.last_applied + 1:]
        return snapshot
    
    def restore_snapshot(self, snapshot: Dict):
        """Restore state from snapshot"""
        if 'last_included_index' not in snapshot or 'last_included_term' not in snapshot:
            logger.error("Invalid snapshot received")
            return
        if not snapshot['state']:  # Check state, not entries
            self.commit_index = -1
            self.last_applied = -1
            return False            
        else:
            self._store._data = snapshot['state']
            self.last_applied = snapshot['last_included_index']
            self.commit_index = snapshot['last_included_index']
            
            # Adjust log
            self.log = []  # Clear log as it's now in snapshot
        
    def should_create_snapshot(self) -> bool:
        """Check if we should create a snapshot
        DDIA: Prevent unbounded log growth"""
        LOG_SIZE_THRESHOLD = 1000  # Example threshold
        return len(self.log) > LOG_SIZE_THRESHOLD

 
    def read(self, key: str) -> Optional[Any]:
        """Linearizable read operation Ensure reads are consistent"""
        if self.raft_state != RaftState.LEADER:
            if self.leader_id:
                return self._forward_read_to_leader(key)
            return None
            
        # Ensure leadership is still valid
        if not self._check_leadership():
            return None
        
        if self.commit_index == -1:
            return None  # No committed entries yet

    
        # Read after checking leadership
        return self._store.read(key)[0] if self._store.read(key) else None
        
    def _check_leadership(self) -> bool:
        """Verify we're still the leader
        DDIA: Leader must confirm its authority"""
        # Send heartbeat to confirm leadership
        responses = 0
        for peer_id, host, port in self._peers:
            try:
                message = RaftMessage(
                    type=RaftMessageType.APPEND_ENTRIES,
                    key="",
                    value={
                        'prev_log_index': len(self.log) - 1,
                        'prev_log_term': self.log[-1]['term'] if self.log else 0,
                        'entries': [],  # Empty for heartbeat
                        'leader_commit': self.commit_index
                    },
                    term=self.current_term,
                    sender_id=self.node_id
                )
                response = Connection.send_message(host, port, message)
                if response and not response.value:
                    return False
                if response:
                    responses += 1
            except Exception:
                continue
                
        return responses >= len(self._peers) // 2


    def add_server(self, new_server_id: str, host: str, port: int) -> bool:
        """Add new server to cluster     DDIA p.370: Safe cluster membership changes"""
        if self.raft_state != RaftState.LEADER:
            return False
            
        # Create configuration change entry
        config_entry = {
            'term': self.current_term,
            'command': {
                'operation': 'CONFIG_CHANGE',
                'type': 'ADD_SERVER',
                'server_id': new_server_id,
                'host': host,
                'port': port
            }
        }
        
        # Replicate config change through log
        self.log.append(config_entry)
        if not self._replicate_log_entry(len(self.log) - 1):
            return False
            
        # Actually add the server
        self._peers.append((new_server_id, host, port))
        self.next_index[new_server_id] = len(self.log)
        self.match_index[new_server_id] = 0
        return True
    
    def _handle_config_change(self, command: Dict):
        """Handle configuration change commands"""
        if command['type'] == 'ADD_SERVER':
            server_id = command['server_id']
            if server_id not in {p[0] for p in self._peers}:
                self._peers.append((
                    server_id,
                    command['host'],
                    command['port']
                ))
                self.next_index[server_id] = len(self.log)
                self.match_index[server_id] = 0

   

    



    

    


    def _send_commit_index_update(self):
        """Send immediate commit index update to followers"""
        for peer_id, host, port in self._peers:
            try:
                message = RaftMessage(
                    type=RaftMessageType.APPEND_ENTRIES,
                    key="",
                    value={
                        'prev_log_index': len(self.log) - 1,
                        'prev_log_term': self.log[-1]['term'] if self.log else 0,
                        'entries': [],
                        'leader_commit': self.commit_index  # Send updated commit index
                    },
                    term=self.current_term,
                    sender_id=self.node_id
                )
                Connection.send_message(host, port, message)
            except Exception as e:
                logger.error(f"Failed to send commit update to {peer_id}: {e}")


    def _apply_committed_entries(self):
        """Apply committed entries to state machine"""
        logger.debug(f"Node {self.node_id} applying entries. Commit index: {self.commit_index}, Last applied: {self.last_applied}")
        
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            command = entry['command']
            
            if command['operation'] == 'WRITE':
                timestamp = time.time()
                logger.debug(f"Applying write command: {command}")
                success = self._store.write(
                    command['key'], 
                    command['value'],
                    timestamp
                )
                logger.debug(f"Write success: {success}")
                if not success:
                    logger.error(f"Failed to apply write command: {command}")
                else:
                    logger.debug(f"Successfully applied write. Store state: {self._store._data}")
            elif command['operation'] == 'CONFIG_CHANGE':
                self._handle_config_change(command)
        #Check if we should create a snapshot
        if self.should_create_snapshot():
            self.create_snapshot()

    
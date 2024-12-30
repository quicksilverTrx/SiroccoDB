# src/replication/single_leader/strategy.py
from typing import Optional, Any, Dict, List, Set
import threading
import time
import logging
from concurrent.futures import ThreadPoolExecutor

from core.interfaces.replication import IReplicationStrategy
from core.types.messages import Message, MessageType
from core.types.states import NodeState

from .types import LogEntry, ConsistencyLevel, ReplicationLogType
from .log_manager import ReplicationLog
from ...core.network.connection import Connection

logger = logging.getLogger(__name__)

class SingleLeaderStrategy(IReplicationStrategy):
    """Single-leader replication with multiple consistency levels"""
    
    def __init__(
        self,
        is_leader: bool,
        consistency_level: ConsistencyLevel = ConsistencyLevel.ASYNC,
        log_type: ReplicationLogType = ReplicationLogType.ROW_BASED,
        catchup_batch_size: int = 1000,
        replication_timeout: float = 5.0
    ):
        self.is_leader = is_leader
        self.consistency_level = consistency_level
        self.replication_log = ReplicationLog(log_type)
        self.catchup_batch_size = catchup_batch_size
        self.replication_timeout = replication_timeout
        
        # Thread management
        self._thread_pool = ThreadPoolExecutor(max_workers=10)
        
        # Follower tracking
        self._follower_positions: Dict[str, int] = {}
        self._sync_followers: Set[str] = set()
        self._lock = threading.Lock()
        
    def __del__(self):
        """Cleanup thread pool on deletion"""
        self._thread_pool.shutdown(wait=False)
        
    def handle_write(self, node: 'INode', key: str, value: Any) -> bool:
        """Handle write with configurable consistency"""
        if not self.is_leader:
            logger.warning(f"Node {node.node_id} is not the leader")
            return False
            
        # Create log entry
        entry = self.replication_log.append({
            'key': key,
            'value': value,
            'operation': 'UPDATE'
        })
        
        # Local write
        success = node._store.write(key, value, entry.timestamp)
        if not success:
            return False
            
        # Handle replication based on consistency level
        if self.consistency_level == ConsistencyLevel.ASYNC:
            self._async_replicate(node, entry)
            return True
            
        elif self.consistency_level == ConsistencyLevel.SEMI_SYNC:
            return self._semi_sync_replicate(node, entry)
            
        else:  # SYNC
            return self._sync_replicate(node, entry)
            
    def _async_replicate(self, node: 'INode', entry: LogEntry):
        """Asynchronous replication using thread pool"""
        for peer_id, host, port in node._peers:
            self._thread_pool.submit(
                self._replicate_to_follower,
                node, peer_id, host, port, entry
            )
            
    def _semi_sync_replicate(self, node: 'INode', entry: LogEntry) -> bool:
        """Semi-synchronous replication (wait for one follower)"""
        event = threading.Event()
        acks = 0
        
        def replicate_and_signal(peer_id: str, host: str, port: int):
            nonlocal acks
            if self._replicate_to_follower(node, peer_id, host, port, entry):
                with self._lock:
                    acks += 1
                    if acks >= 1:
                        event.set()
                        
        # Start replication threads
        for peer_id, host, port in node._peers:
            self._thread_pool.submit(
                replicate_and_signal,
                peer_id, host, port
            )
            
        # Wait for at least one acknowledgment
        return event.wait(timeout=self.replication_timeout)
        
    def _sync_replicate(self, node: 'INode', entry: LogEntry) -> bool:
        """Synchronous replication (wait for all followers)"""
        successes = 0
        required_acks = len(node._peers)
        
        for peer_id, host, port in node._peers:
            if self._replicate_to_follower(node, peer_id, host, port, entry):
                successes += 1
                
        return successes >= required_acks
        
    def _replicate_to_follower(
        self, 
        node: 'INode', 
        peer_id: str, 
        host: str, 
        port: int, 
        entry: LogEntry
    ) -> bool:
        """Replicate a single entry to a follower"""
        try:
            message = Message(
                type=MessageType.REPLICATE,
                key=entry.key,
                value=entry.value,
                timestamp=entry.timestamp,
                metadata={'sequence_number': entry.sequence_number}
            )
            
            response = Connection.send_message(host, port, message)
            if response and response.value:
                with self._lock:
                    self._follower_positions[peer_id] = entry.sequence_number
                return True
                
        except Exception as e:
            pass
            
        return False
        
    def handle_read(self, node: 'INode', key: str) -> Optional[Any]:
        """Read handling with basic read-your-writes consistency"""
        try:
            result = node._store.read(key)
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Read operation failed: {e}")
            return None
            
    def handle_new_follower(self, node: 'INode', follower_id: str) -> bool:
        """Handle new follower setup"""
        try:
            # Create and send snapshot
            snapshot = self.replication_log.create_snapshot()
            message = Message(
                type=MessageType.SNAPSHOT,
                key="",
                value=snapshot
            )
            
            # Find follower connection details
            follower_info = next(
                (p for p in node._peers if p[0] == follower_id),
                None
            )
            
            if not follower_info:
                return False
                
            _, host, port = follower_info
            response = Connection.send_message(host, port, message)
            
            if response and response.value:
                with self._lock:
                    self._follower_positions[follower_id] = snapshot['sequence_number']
                return True
                
        except Exception as e:
            logger.error(f"New follower setup failed: {e}")
            
        return False
        
    def handle_follower_catchup(self, node: 'INode', follower_id: str, last_sequence: int):
        """Handle follower catchup after outage"""
        try:
            entries = self.replication_log.get_entries_after(last_sequence)
            
            # Send entries in batches
            for i in range(0, len(entries), self.catchup_batch_size):
                batch = entries[i:i + self.catchup_batch_size]
                message = Message(
                    type=MessageType.CATCHUP,
                    key="",
                    value=batch
                )
                
                follower_info = next(
                    (p for p in node._peers if p[0] == follower_id),
                    None
                )
                
                if not follower_info:
                    return
                    
                _, host, port = follower_info
                response = Connection.send_message(host, port, message)
                
                if not response or not response.value:
                    logger.error(f"Catchup failed for {follower_id}")
                    return
                    
            with self._lock:
                self._follower_positions[follower_id] = entries[-1].sequence_number
                
        except Exception as e:
            logger.error(f"Follower catchup failed: {e}")
            
    def handle_message(self, node: 'INode', message: Message) -> Optional[Message]:
        """Handle incoming messages"""
        handlers = {
            MessageType.WRITE: self._handle_write_message,
            MessageType.READ: self._handle_read_message,
            MessageType.REPLICATE: self._handle_replicate_message,
            MessageType.CATCHUP: self._handle_catchup_message,
            MessageType.SNAPSHOT: self._handle_snapshot_message
        }
        
        handler = handlers.get(message.type)
        if not handler:
            logger.warning(f"Unhandled message type: {message.type}")
            return None
            
        try:
            return handler(node, message)
        except Exception as e:
            logger.error(f"Message handling failed: {e}")
            return None
            
    def _handle_write_message(self, node: 'INode', message: Message) -> Message:
        """Handle write message"""
        success = self._store.write(message.key, message.value, message.timestamp)
        return Message(
            type=MessageType.WRITE_RESPONSE,
            key=message.key,
            value=success,
            sender_id=node.node_id
        )
        
    def _handle_read_message(self, node: 'INode', message: Message) -> Message:
        """Handle read message"""
        value = self._store.read(message.key)
        return Message(
            type=MessageType.READ_RESPONSE,
            key=message.key,
            value=value[0] if value else None,
            timestamp=value[1] if value else None,
            sender_id=node.node_id
        )
        
    def _handle_replicate_message(self, node: 'INode', message: Message) -> Message:
        """Handle replication message"""
        success = node._store.write(message.key, message.value, message.timestamp)
        if success and message.metadata:
            sequence_number = message.metadata.get('sequence_number')
            if sequence_number:
                with self._lock:
                    self._follower_positions[node.node_id] = sequence_number
                    
        return Message(
            type=MessageType.WRITE_RESPONSE,
            key=message.key,
            value=success,
            sender_id=node.node_id
        )
        
    def _handle_catchup_message(self, node: 'INode', message: Message) -> Message:
        """Handle catchup data from leader"""
        success = True
        for entry in message.value:
            if not node._store.write(entry.key, entry.value, entry.timestamp):
                success = False
                break
                
        return Message(
            type=MessageType.CATCHUP_RESPONSE,
            value=success,
            sender_id=node.node_id
        )
        
    def _handle_snapshot_message(self, node: 'INode', message: Message) -> Message:
        """Handle snapshot data from leader"""
        try:
            self.replication_log.restore_from_snapshot(message.value)
            success = True
        except Exception as e:
            logger.error(f"Failed to restore from snapshot: {e}")
            success = False
            
        return Message(
            type=MessageType.SNAPSHOT_RESPONSE,
            value=success,
            sender_id=node.node_id
        )
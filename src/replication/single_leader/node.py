from typing import Optional, Any
from core.node import DistributedKeyValueStoreNode
from core.types.states import NodeState
from core.types.messages import Message, MessageType
from .strategy import SingleLeaderReplicationStrategy

class SingleLeaderNode(DistributedKeyValueStoreNode):
    """
    Node implementation for single-leader replication pattern.
    Extends base node with leader-specific functionality.
    """
    def _init_(self,  node_id: str, host: str, port: int, is_leader: bool = False,  sync_replicas: int = 1):
        super().__init__(node_id, host, port)
        self.replication_strategy = SingleLeaderReplicationStrategy( is_leader=is_leader,
        sync_replicas=sync_replicas)
        
        self.is_leader = is_leader
        self.leader_id : Optional[str] = node_id if is_leader else None
        self.last_heartbeat : float = 0

    def promote_to_leader(self):
        """Promote this node to leader"""
        self.is_leader = True
        self.leader_id = self.node_id
        self.replication_strategy.is_leader = True

    def demote_from_leader(self):
        """Demote this node from leader"""
        self.is_leader = False
        self.leader_id = None
        self.replication_strategy.is_leader = False

    def send_heartbeat(self):
        """Send a heartbeat message to all followers"""
        if not self.is_leader:
            return
        
        message = Message(type=MessageType.HEARTBEAT, key = "", sender_id=self.node_id, timestamp = time.time())
        for peer_id, host, port in self.peers:
            try :
                Connection.send_message(host, port, message)
            except:
                pass
    
    def _handle_message(self, message : Message) -> Optional[Message]:
        """Handle incoming messages"""
        if message.type == MessageType.HEARTBEAT:
            self.last_heartbeat = message.timestamp
            return None
        return super()._handle_message(message)
    
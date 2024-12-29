import socket
from .interfaces.node import INode
from .storage.memory_store import MemoryStore
from .types.states import NodeState
from typing import List, Tuple, Any,Optional
import threading
from .network.connection import Connection

class DistributedKeyValueStoreNode(INode):
    """Base Node implementation for a distributed key-value store"""

    def _init_(self, node_id : str, host : str, port : int):
        self.node_id = node_id
        self.host = host
        self.port = port

        self._store = MemoryStore()
        self.state = NodeState.INACTIVE
        self._peers = List[Tuple[str,str,int]] = []

        #network
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._running = False



    def start(self):
        """ Sets up the socket, starts listening, and launches the _listen method in a separate thread"""
        self._socket.bind((self.host, self.port))
        self._socket.listen(5) 
        self._running = True
        self.state = NodeState.ACTIVE

        self._listener_thread = threading.Thread(target=self._listen) # Start the listener thread with target as the _listen method
        self._listener_thread.daemon = True # Ensure thread stops when the main program exits
        self._listener_thread.start() # Start the listener thread


    def stop(self):
        self._running = False
        self.state = NodeState.INACTIVE
        self._socket.close()

    def add_peer (self,node_id : str, node_host : str, node_port : int):
        self._peers.append((node_id, node_host, node_port))

    def write (self, key : str, value : Any) -> bool:
        if self.state != NodeState.ACTIVE:
            return False
        if self.replication_strategy:
            return self.replication_strategy.handle_write(self,key, value)
        return self._store.write(key, value)
    
    def read(self, key : str) -> Optional[Any]:
        if self.state != NodeState.ACTIVE:
            return None
        if self.replication_strategy:
            return self.replication_strategy.handle_read(self, key)
        result =  self._store.read(key)
        return result [0] if result else None
    
    def _listen(self):
        """Runs in a continuous loop, accepting new connections and delegating them to individual handler threads.
          _listen runs in the listener thread, accepting client connections one at a time.
          For each connection, _listen spawns a handler thread, which runs _handle_connection to process the communication."""
        while self.running:
            try:
                client_socket, _ = self._socket.accept() # Accept an incoming connection and Returns a new client_socket for communication 
                handler = threading.Thread(target=self._handle_connection, args=(client_socket,))  # Pass the accepted socket to the handler
                handler.daemon = True  # Ensure thread stops when the main program exits
                handler.start() # Start a new thread to handle the connection
            except Exception as e:
                    print(f"Error accepting connection: {e}")
    
    def _handle_connection(self, client_socket : socket.socket):
        try:
            message= Connection.receive_message(client_socket)
            if message and self.replcation_strategy:
                response = self.replication_strategy.handle_message(self, message)
                if response:
                    Connection.send_message(client_socket, response)
        except Exception as e:
            print(f"Error handling connection: {e}")
        finally:
            client_socket.close()
                




#src/core/node.py
import socket
from .interfaces.node import INode
from .storage.memory_store import MemoryStore
from .types.states import NodeState
from typing import List, Tuple, Any,Optional
import threading
from .network.connection import Connection
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class DistributedKeyValueStoreNode(INode):
    """Base Node implementation for a distributed key-value store"""

    def __init__(self, node_id : str, host : str, port : int):
        self.node_id = node_id
        self.host = host
        self.port = port

        self._store = MemoryStore()
        self.state = NodeState.INACTIVE
        self._peers : List[Tuple[str,str,int]] = []

        #network
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._running = False
        self._shutting_down = False



    def start(self):
        """Sets up the socket, starts listening, and launches the _listen method in a separate thread."""
        try:

            if self._running:
                logger.warning(f"Node {self.node_id} is already running.")
                return
            # Ensure host is valid, fallback to "127.0.0.1" if not set
            self.host = self.host or "127.0.0.1"
            
            # Validate port range
            if not (1024 <= self.port <= 65535):
                raise ValueError(f"Port {self.port} is out of the valid range (1024-65535)")

            # Add socket reuse options
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                logger.warning("SO_REUSEPORT is not supported on this platform.")
            
            # Attempt to bind socket with retries
            max_retries = 5  # Max retries for port binding
            for attempt in range(max_retries):
                try:
                    logger.info(f"Attempting to bind node {self.node_id} to {self.host}:{self.port}")
                    self._socket.bind((self.host, self.port))
                    break  # Exit loop on success
                except OSError as oe:
                    if oe.errno == 22:  # Invalid argument
                        logger.error(f"Invalid host or port for node {self.node_id}: {self.host}:{self.port}")
                        raise
                    elif oe.errno == 48:  # Address already in use
                        logger.warning(f"Port {self.port} already in use for node {self.node_id}. Retrying with next port...")
                        self.port += 1
                    else:
                        logger.error(f"OS error starting node {self.node_id}: {oe}")
                        raise
            else:
                raise OSError(f"Failed to bind node {self.node_id} after {max_retries} attempts.")

            # Start listening
            self._socket.listen(5)
            self._running = True
            self.state = NodeState.ACTIVE
            logger.info(f"Node {self.node_id} successfully started on {self.host}:{self.port}")

            # Start the listener thread
            self._listener_thread = threading.Thread(target=self._listen)
            self._listener_thread.daemon = True  # Ensure thread stops when the main program exits
            self._listener_thread.start()

        except ValueError as ve:
            logger.error(f"Validation error for node {self.node_id}: {ve}")
            self.state = NodeState.INACTIVE
            self._running = False

        except OSError as oe:
            logger.error(f"OS error for node {self.node_id}: {oe}")
            self.state = NodeState.INACTIVE
            self._running = False

        except Exception as e:
            logger.error(f"Unexpected error starting node {self.node_id}: {e}")
            logger.error("Traceback:", exc_info=True)
            self.state = NodeState.INACTIVE
            self._running = False

    def stop(self):
        """Gracefully stop the node."""
        logger.info(f"Stopping node {self.node_id}")
        #self._shutting_down = True  # Add a shutdown flag
        self._running = False
        self.state = NodeState.INACTIVE
        try:
            self._socket.close()
        except Exception as e:
            logger.error(f"Error closing socket for {self.node_id}: {e}")

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
        while self._running:
            try:
                logger.debug(f"Listening for connections on {self.host}:{self.port}")
                client_socket, _ = self._socket.accept() # Accept an incoming connection and Returns a new client_socket for communication 
                # if self._shutting_down:  # Skip processing if shutting down
                #     break
                logger.debug(f"Accepted connection from {client_socket.getpeername()}")
                handler = threading.Thread(target=self._handle_connection, args=(client_socket,))  # Pass the accepted socket to the handler
                handler.daemon = True  # Ensure thread stops when the main program exits
                handler.start() # Start a new thread to handle the connection
            except Exception as e:
                    # if  self._shutting_down:  # Suppress errors during shutdown
                    #     break
                    logger.error(f"Error accepting connection: {e}")
    
    def _handle_connection(self, client_socket: socket.socket):
        try:
            message = Connection.receive_message(client_socket)
            logger.debug(f"Received message {message}")
            if message and hasattr(self, 'replication_strategy') and self.replication_strategy:
                response = self.replication_strategy.handle_message(self, message)
                if response:
                    Connection.send_message(client_socket, response)
        except Exception as e:
            logger.error(f"Error handling connection: {e}")
        finally:
            client_socket.close()
                




#/src/core/network/connection.py
import socket
import pickle
from typing import Optional
from ..types.messages import Message
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class Connection:
    
    TIMEOUT = 5.0    # Socket timeout in seconds
    BUFFER = 4096    # Buffer size for messages
    
    @staticmethod
    def send_message(host: str, port: int, message: Message) -> Optional[Message]:
        """Send a message to a remote node"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(Connection.TIMEOUT)
                s.connect((host, port))
                s.sendall(pickle.dumps(message))
                #logger.debug(f"Sent message {message} to {host}:{port}")
                data = s.recv(Connection.BUFFER)
                return pickle.loads(data) if data else None
        except Exception as e:
            logger.error(f"Error sending message to {host}:{port} - {e}")
            logger.error (e)
    
    @staticmethod
    def receive_message(client_socket: socket.socket) -> Optional[Message]:
        """Receive a message from a socket"""
        try:
            data = client_socket.recv(Connection.BUFFER)
            #logger.debug(f"Received {data}")
            return pickle.loads(data) if data else None
        except Exception as e:
            logger.error(e)

    @staticmethod
    def send_raw_message(client_socket: socket.socket, message: Message) -> None:
        """Send a message over an already established socket connection."""
        try:
            client_socket.sendall(pickle.dumps(message))
            logger.debug(f"Sent message {message} over existing socket")
        except Exception as e:
            logger.error(f"Error sending message over socket: {e}")
            raise
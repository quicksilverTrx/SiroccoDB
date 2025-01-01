#/src/core/network/connection.py
import socket
import pickle
from typing import Optional
from ..types.messages import Message
import logging

logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger("src.core.network.connection")
logger.setLevel(logging.CRITICAL)
class Connection:
    
    TIMEOUT = 0.5    # Socket timeout in seconds
    BUFFER = 4096    # Buffer size for messages
    
    @staticmethod
    def send_message(host: str, port: int, message: Message) -> Optional[Message]:
        """Send a message to a remote node"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(Connection.TIMEOUT)
                try:
                    s.connect((host, port))
                except (ConnectionRefusedError, socket.timeout):
                    # Silently fail on connection refused or timeout
                    return None
                except Exception as e:
                # Log other errors
                    logger.error(f"Error sending message to {host}:{port} - {e}")
                    return None
                s.sendall(pickle.dumps(message))
                #logger.debug(f"Sent message {message} to {host}:{port}")
                try:
                    data = s.recv(Connection.BUFFER)
                except socket.timeout:
                    # Silently fail on timeout
                    return None
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
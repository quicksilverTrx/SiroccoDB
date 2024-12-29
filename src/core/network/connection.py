import socket
import pickle
from typing import Optional
from ..types.messages import Message

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
                data = s.recv(Connection.BUFFER)
                return pickle.loads(data) if data else None
        except Exception as e:
            print (e)
    
    @staticmethod
    def receive_message(client_socket: socket.socket) -> Optional[Message]:
        """Receive a message from a socket"""
        try:
            data = client_socket.recv(Connection.BUFFER)
            return pickle.loads(data) if data else None
        except Exception as e:
            print(e)
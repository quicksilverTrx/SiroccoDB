from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any
import time

class MessageType(Enum):
    WRITE = "WRITE"
    READ = "READ"
    WRITE_RESPONSE = "WRITE_RESPONSE"
    READ_RESPONSE = "READ_RESPONSE"

@dataclass
class Message:
    """Message format for node communication"""
    type: MessageType
    key: str
    value: Optional[Any] = None
    timestamp: float = time.time()

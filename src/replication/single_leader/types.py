from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, Optional
import time

class ConsistencyLevel(Enum):
    ASYNC = "async"  # Leader doesn't wait
    SEMI_SYNC = "semi_sync"  # Wait for one follower
    SYNC = "sync"  # Wait for all followers

class ReplicationLogType(Enum):
    STATEMENT = "statement"  # SQL or operation statements
    WAL = "wal"  # Write-ahead log
    ROW_BASED = "row_based"  # Logical row changes
    TRIGGER = "trigger"  # Trigger-based changes

class LogEntry:
    """Base class for log entries"""
    sequence_number: int
    timestamp: float

@dataclass
class StatementLogEntry(LogEntry):
    """For statement-based replication"""
    statement: str
    parameters: Dict[str, Any]

@dataclass
class WALLogEntry(LogEntry):
    """For write-ahead logging"""
    before_image: Dict[str, Any]
    after_image: Dict[str, Any]
    operation: str

@dataclass
class RowLogEntry(LogEntry):
    """For row-based replication"""
    key: str
    value: Any
    operation: str  # INSERT, UPDATE, DELETE

@dataclass
class TriggerLogEntry(LogEntry):
    """For trigger-based replication"""
    trigger_name: str
    old_value: Optional[Any]
    new_value: Optional[Any]
    metadata: Dict[str, Any]
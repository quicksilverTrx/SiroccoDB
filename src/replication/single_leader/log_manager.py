
import threading
from typing import List, Dict, Any, Optional, Type
from collections import deque
import time

from .types import (
    ReplicationLogType, LogEntry, StatementLogEntry, 
    WALLogEntry, RowLogEntry, TriggerLogEntry
)


class ReplicationLog:
    def __init__(self, log_type: ReplicationLogType):
        self.log_type = log_type
        self.entries: deque[LogEntry] = deque()
        self._lock = threading.Lock()
        self._sequence_number = 0
        
    def append(self, entry_data: Dict[str, Any]) -> LogEntry:
        """Add a new entry to the log"""
        with self._lock:
            self._sequence_number += 1
            entry = self._create_entry(entry_data)
            self.entries.append(entry)
            return entry
            
    def get_entries_after(self, sequence_number: int) -> List[LogEntry]:
        """Get all entries after a given sequence number"""
        with self._lock:
            return [e for e in self.entries if e.sequence_number > sequence_number]
            
    def truncate_before(self, sequence_number: int):
        """Remove entries before a given sequence number"""
        with self._lock:
            while self.entries and self.entries[0].sequence_number < sequence_number:
                self.entries.popleft()
                
    def _create_entry(self, data: Dict[str, Any]) -> LogEntry:
        """Create appropriate log entry based on type"""
        entry_types = {
            ReplicationLogType.STATEMENT: StatementLogEntry,
            ReplicationLogType.WAL: WALLogEntry,
            ReplicationLogType.ROW_BASED: RowLogEntry,
            ReplicationLogType.TRIGGER: TriggerLogEntry
        }
        
        EntryClass = entry_types[self.log_type]
        return EntryClass(
            sequence_number=self._sequence_number,
            timestamp=time.time(),
            **data
        )
        
    def create_snapshot(self) -> Dict[str, Any]:
        """Create a snapshot of current state"""
        with self._lock:
            return {
                'sequence_number': self._sequence_number,
                'entries': list(self.entries)
            }
            
    def restore_from_snapshot(self, snapshot: Dict[str, Any]):
        """Restore state from a snapshot"""
        with self._lock:
            self._sequence_number = snapshot['sequence_number']
            self.entries = deque(snapshot['entries'])
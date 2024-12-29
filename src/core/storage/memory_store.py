from typing import Dict, Any, Optional, Tuple
import threading
import time

class MemoryStore:
    """In-memory key-value store with thread-safe operations"""

    def __init__ (self):
        self.data: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.Lock()

    def write(self, key : str, value : Any, timestamp : float) -> bool:
        with self._lock:
            current = self._data.get(key, (None, 0))
            if not current or current[1] < timestamp:
                self._data[key] = (value, timestamp)
                return True
            return False
        
    def read(self, key : str) -> Optional[Tuple[Any, float]]:
        with self._lock:
            return self._data.get(key)
    
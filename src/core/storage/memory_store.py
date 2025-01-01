from typing import Dict, Any, Optional, Tuple
import threading
import time

class MemoryStore:
    """In-memory key-value store with thread-safe operations"""
    
    def __init__(self):
        self._data: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.Lock()
        
    def write(self, key: str, value: Any, timestamp: float = None) -> bool:
        """Thread-safe write operation"""
        if timestamp is None:
            timestamp = time.time()
            
        with self._lock:
            current = self._data.get(key)
            if not current or current[1] < timestamp:
                self._data[key] = (value, timestamp)
                return True
            return False
            
    def read(self, key: str) -> Optional[Tuple[Any, float]]:
        """Thread-safe read operation"""
        with self._lock:
            return self._data.get(key)

    def debug_dump(self) -> Dict:
        """Debug helper to dump store contents"""
        with self._lock:
            return {k: v[0] for k, v in self._data.items()}
    
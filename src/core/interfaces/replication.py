from abc import ABC,abstractmethod
from typing import Any, Optional

class IReplicationStrategy(ABC):
    """Interface for replication strategies"""
    @abstractmethod
    def handle_write(self, node :'INode', key : str, value : Any) -> bool:
        pass

    @abstractmethod
    def handle_read(self, node: 'INode', key : str) -> Optional[Any]:
        pass
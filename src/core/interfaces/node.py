from abc import ABC,abstractmethod
from typing import Optional, Any

class INode(ABC):
    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass

    @abstractmethod
    def write(self, key : str, value : Any) -> bool:
        pass

    @abstractmethod
    def read(self, key : str) -> Optional[Any]:
        pass


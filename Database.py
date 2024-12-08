import threading
from typing import Dict, Tuple, Any

class Database:
    def __init__(self):
        self.data: Dict[str, Tuple[Any, int]] = {}  # (value, version)
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Tuple[Any, int]:
        with self.lock:
            return self.data.get(key, (None, 0))
    
    def set(self, key: str, value: Any, version: int):
        with self.lock:
            self.data[key] = (value, version)
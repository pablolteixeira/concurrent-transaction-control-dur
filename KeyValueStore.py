from typing import Dict, Optional, Tuple, Any


class KeyValueStore:

    def __init__(self):
        self.store: Dict[str, Tuple[Any, int]] = {}

    def read(self, key: str) -> Optional[Tuple[Any, int]]:

        return self.store.get(key)

    def write(self, key: str, value: Any, version: int):

        self.store[key] = (value, version)


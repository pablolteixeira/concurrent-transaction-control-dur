from typing import Dict, Tuple, Any

class Transaction:
    def __init__(self, cid: int, tid: int):
        self.cid = cid  # Client ID
        self.tid = tid  # Transaction ID
        self.ws: Dict[str, Any] = {}  # Write set: {key: value}
        self.rs: Dict[str, Tuple[Any, int]] = {}  # Read set: {key: (value, version)}
        self.result = None
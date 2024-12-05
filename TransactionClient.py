import socket
import json
import uuid
import logging
from typing import Any, Dict, Tuple

logger = logging.getLogger("TransactionClient")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s"
    )
)
if not logger.handlers:
    logger.addHandler(ch)


class TransactionClient:
    def __init__(self, server_address: Tuple[str, int]):
        self.server_address = server_address
        self.transaction_id = None
        self.rs = []
        self.ws = {}
        self.socket = None
        self._connect_to_server()

    def _connect_to_server(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(self.server_address)
        self.socket.settimeout(15)

    def start_transaction(self):
        self.transaction_id = str(uuid.uuid4())
        self.rs.clear()
        self.ws.clear()

    def read(self, item: str, version: int = None) -> Any:
        if item in self.ws:
            return self.ws[item]
        request = {
            "type": "read",
            "transaction_id": self.transaction_id,
            "item": item,
            "version": version,
        }
        self._send_request_to_server(request)
        response = self._receive_response_from_server()
        value = response.get("value")
        server_version = response.get("version")
        self.rs.append((item, server_version or 0))
        return value

    def write(self, item: str, value: Any):
        self.ws[item] = value

    def commit(self) -> bool:
        request = {
            "type": "commit",
            "transaction_id": self.transaction_id,
            "rs": self.rs,
            "ws": list(self.ws.items()),
        }
        self._send_request_to_server(request)
        response = self._receive_response_from_server()
        if response.get("status") == "committed":
            self._reset_transaction()
            return True
        return False

    def abort(self):
        request = {"type": "abort", "transaction_id": self.transaction_id}
        try:
            self._send_request_to_server(request)
            self._receive_response_from_server()
        finally:
            self._reset_transaction()

    def _send_request_to_server(self, request: Dict):
        self.socket.sendall(json.dumps(request).encode())

    def _receive_response_from_server(self) -> Dict:
        response = self.socket.recv(4096).decode()
        if not response:
            raise ConnectionError("Empty response from server.")
        return json.loads(response)

    def _reset_transaction(self):
        self.transaction_id = None
        self.rs.clear()
        self.ws.clear()

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

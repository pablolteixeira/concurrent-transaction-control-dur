import socket
import json
import uuid
from typing import Any, Dict, List, Tuple
import threading


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

    def start_transaction(self):
        self.transaction_id = str(uuid.uuid4())
        self.rs = []
        self.ws = {}

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
        if version:
            self.rs.append((item, version))
        else:
            self.rs.append((item, None))
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
        commit_status = response.get("status") == "committed"
        if commit_status:
            self._reset_transaction()
        return commit_status

    def abort(self):
        request = {"type": "abort", "transaction_id": self.transaction_id}
        self._send_request_to_server(request)
        self._receive_response_from_server()
        self._reset_transaction()

    def _send_request_to_server(self, request: Dict):
        message = json.dumps(request).encode()
        self.socket.sendall(message)

    def _receive_response_from_server(self) -> Dict:
        response = self.socket.recv(4096).decode()
        return json.loads(response)

    def _reset_transaction(self):
        self.transaction_id = None
        self.rs = []
        self.ws = {}

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

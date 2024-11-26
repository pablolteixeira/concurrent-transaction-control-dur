import socket
import json
import uuid
from typing import Any, Dict, List, Tuple


class TransactionClient:

    def __init__(self, server_addresses: List[Tuple[str, int]]):

        self.server_addresses = server_addresses
        self.transaction_id = None
        self.rs = []  # Read set: List of tuples (item, version).
        self.ws = {}  # Write set: Dictionary of item -> value.
        self.sockets = []
        self._connect_to_servers()

    def _connect_to_servers(self):

        for address in self.server_addresses:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(address)
            self.sockets.append(s)

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
        self._send_request_to_servers(request)
        response = self._receive_response_from_servers()

        value = response[0].get("value")
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
        self._send_request_to_servers(request)
        response = self._receive_response_from_servers()

        commit_status = all(resp.get("status") == "committed" for resp in response)
        if commit_status:
            self._reset_transaction()
        return commit_status

    def abort(self):

        request = {"type": "abort", "transaction_id": self.transaction_id}
        self._send_request_to_servers(request)
        self._receive_response_from_servers()
        self._reset_transaction()

    def _send_request_to_servers(self, request: Dict):
        message = json.dumps(request).encode()
        for sock in self.sockets:
            sock.sendall(message)

    def _receive_response_from_servers(self) -> List[Dict]:

        responses = []
        for sock in self.sockets:
            response = sock.recv(4096).decode()
            responses.append(json.loads(response))
        return responses

    def _reset_transaction(self):

        self.transaction_id = None
        self.rs = []
        self.ws = {}

    def close(self):
        for sock in self.sockets:
            sock.close()
        self.sockets = []

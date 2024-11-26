import socket
import json
import threading
from typing import Dict, Tuple, List


class KeyValueStore:

    def __init__(self):
        self.store = {}

    def read(self, key: str) -> Tuple:
        return self.store.get(key, None)

    def write(self, key: str, value: any, version: int):
        self.store[key] = (value, version)


class ReplicationServer:

    def __init__(self, host: str, port: int):

        self.host = host
        self.port = port
        self.database = KeyValueStore()
        self.server_socket = None
        self.lock = threading.Lock()

    def start(self):

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Replication Server listening on {self.host}:{self.port}")

        while True:
            client_socket, address = self.server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client_socket,)).start()

    def handle_request(self, client_socket: socket.socket):

        try:
            request = client_socket.recv(4096).decode()
            data = json.loads(request)

            response = None
            if data["type"] == "read":
                response = self._handle_read(data)
            elif data["type"] == "write":
                response = self._handle_write(data)
            elif data["type"] == "commit":
                response = self._handle_commit(data)
            elif data["type"] == "abort":
                response = self._handle_abort(data)

            if response:
                client_socket.sendall(json.dumps(response).encode())

        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            client_socket.close()

    def _handle_read(self, data: Dict):

        key = data["item"]
        result = self.database.read(key)
        if result:
            value, version = result
            return {"status": "success", "value": value, "version": version}
        return {"status": "error", "message": "Key not found"}

    def _handle_write(self, data: Dict):

        return {"status": "success"}

    def _handle_commit(self, data: Dict):

        rs = data["rs"]
        ws = data["ws"]

        if self.certify_transaction(rs, ws):
            self.apply_transaction(ws)
            return {"status": "committed"}
        return {"status": "aborted"}

    def _handle_abort(self, data: Dict):

        return {"status": "aborted"}

    def certify_transaction(
        self, rs: List[Tuple[str, int]], ws: List[Tuple[str, any]]
    ) -> bool:
        with self.lock:
            for item, version in rs:
                current = self.database.read(item)
                if current and current[1] != version:
                    return False
        return True

    def apply_transaction(self, ws: List[Tuple[str, any]]):

        with self.lock:
            for key, value in ws:
                current = self.database.read(key)
                version = current[1] + 1 if current else 1
                self.database.write(key, value, version)

    def stop(self):

        if self.server_socket:
            self.server_socket.close()

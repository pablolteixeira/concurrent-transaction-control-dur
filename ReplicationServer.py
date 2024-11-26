import socket
import json
import threading
from typing import Dict, Tuple, List, Optional
from PaxosNode import PaxosNode


class KeyValueStore:
    def __init__(self):
        self.store = {}

    def read(self, key: str) -> Optional[Tuple]:
        return self.store.get(key)

    def write(self, key: str, value: any, version: int):
        self.store[key] = (value, version)


class ReplicationServer:
    def __init__(
        self,
        host: str,
        port: int,
        node_id: int,
        server_addresses: List[Tuple[str, int]],
        paxos_port: int,
    ):
        self.host = host
        self.port = port
        self.database = KeyValueStore()
        self.lock = threading.Lock()
        self.pending_commits = {}
        self.node_id = node_id
        self.server_addresses = server_addresses
        self.paxos_port = paxos_port
        self.paxos_node = PaxosNode(node_id, server_addresses, paxos_port)
        self.server_socket = None

        self.paxos_node.start()
        threading.Thread(target=self._process_delivered_messages, daemon=True).start()

    def start(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            print(f"Replication Server listening on {self.host}:{self.port}")

            while True:
                client_socket, _ = self.server_socket.accept()
                threading.Thread(
                    target=self.handle_request, args=(client_socket,), daemon=True
                ).start()
        except Exception as e:
            print(f"Error starting server: {e}")
        finally:
            self.stop()

    def handle_request(self, client_socket: socket.socket):
        try:
            request = client_socket.recv(4096).decode()
            data = json.loads(request)

            if data["type"] == "read":
                response = self._handle_read(data)
                client_socket.sendall(json.dumps(response).encode())
            elif data["type"] == "commit":
                self._handle_commit_request(data, client_socket)
            elif data["type"] == "abort":
                response = self._handle_abort(data)
                client_socket.sendall(json.dumps(response).encode())
        except json.JSONDecodeError:
            print("Received invalid JSON request")
            client_socket.sendall(
                json.dumps(
                    {"status": "error", "message": "Invalid request format"}
                ).encode()
            )
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            client_socket.close()

    def _handle_read(self, data: Dict) -> Dict:
        key = data["item"]
        result = self.database.read(key)
        if result:
            value, version = result
            return {"status": "success", "value": value, "version": version}
        return {"status": "error", "message": "Key not found"}

    def _handle_commit_request(self, data: Dict, client_socket: socket.socket):
        transaction_id = data["transaction_id"]
        with self.lock:
            self.pending_commits[transaction_id] = client_socket
        self.paxos_node.propose(data)

    def _handle_abort(self, data: Dict) -> Dict:
        return {"status": "aborted"}

    def _process_delivered_messages(self):
        while True:
            message = self.paxos_node.deliver()
            if message:
                if message["type"] == "commit":
                    self._handle_commit(message)

    def _handle_commit(self, data: Dict):
        transaction_id = data["transaction_id"]
        rs = data["rs"]
        ws = data["ws"]

        commit_status = self.certify_transaction(rs, ws)
        if commit_status:
            self.apply_transaction(ws)
            response = {"status": "committed"}
        else:
            response = {"status": "aborted"}

        # Send the response back to the client
        with self.lock:
            client_socket = self.pending_commits.pop(transaction_id, None)
        if client_socket:
            try:
                client_socket.sendall(json.dumps(response).encode())
            except Exception as e:
                print(f"Error sending response to client: {e}")
            finally:
                client_socket.close()

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
            self.server_socket = None
            print("Replication Server stopped.")

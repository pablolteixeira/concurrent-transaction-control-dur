import socket
import json
import threading
import logging
from typing import Dict, Tuple, List, Any
from PaxosNode import PaxosNode


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
        self.node_id = node_id
        self.server_addresses = server_addresses
        self.paxos_port = paxos_port
        self.database = {}
        self.lock = threading.Lock()
        self.pending_commits: Dict[str, socket.socket] = {}
        self.paxos_node = PaxosNode(node_id, server_addresses, paxos_port)
        self.server_socket = None
        self.paxos_node.start()
        threading.Thread(
            target=self._process_delivered_messages,
            name=f"DeliveredMessagesThread-{self.node_id}",
            daemon=True,
        ).start()

    def start(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            while True:
                try:
                    client_socket, _ = self.server_socket.accept()
                    threading.Thread(
                        target=self.handle_request,
                        args=(client_socket,),
                        name=f"ClientHandler-{client_socket.getpeername()}",
                        daemon=True,
                    ).start()
                except Exception as e:
                    print(e)
        except Exception as e:
            self.stop()

    def handle_request(self, client_socket: socket.socket):
        thread_name = threading.current_thread().name
        try:
            request = client_socket.recv(4096).decode()
            if not request:
                return
            data = json.loads(request)
            request_type = data.get("type")
            if request_type == "read":
                self._handle_read(data, client_socket)
            elif request_type == "commit":
                self._handle_commit_request(data, client_socket)
                return
            elif request_type == "abort":
                self._handle_abort_request(data, client_socket)
            else:
                self._send_response(
                    client_socket,
                    {"status": "error", "message": "Unknown request type"},
                )
        except json.JSONDecodeError:
            self._send_response(
                client_socket, {"status": "error", "message": "Invalid request format"}
            )
        except Exception:
            self._send_response(
                client_socket, {"status": "error", "message": "Internal server error"}
            )
        finally:
            if data.get("type") != "commit":
                self._close_socket(client_socket)

    def _handle_read(self, data: Dict, client_socket: socket.socket):
        key = data.get("item")
        result = self.database.get(key)
        if result:
            value, version = result
            response = {"status": "success", "value": value, "version": version}
        else:
            response = {"status": "success", "value": None, "version": 0}
        self._send_response(client_socket, response)

    def _handle_commit_request(self, data: Dict, client_socket: socket.socket):
        transaction_id = data.get("transaction_id")
        if not transaction_id:
            self._send_response(
                client_socket,
                {"status": "error", "message": "Missing transaction ID"},
                client_socket,
            )
            return
        with self.lock:
            if transaction_id in self.pending_commits:
                self._send_response(
                    client_socket,
                    {"status": "error", "message": "Transaction already pending"},
                    client_socket,
                )
                return
            self.pending_commits[transaction_id] = client_socket
        try:
            self.paxos_node.propose(data)
        except Exception:
            self._send_response(
                client_socket,
                {"status": "error", "message": "Failed to propose transaction"},
            )
            with self.lock:
                self.pending_commits.pop(transaction_id, None)
            self._close_socket(client_socket)

    def _handle_abort_request(self, data: Dict, client_socket: socket.socket):
        transaction_id = data.get("transaction_id")
        if not transaction_id:
            self._send_response(
                client_socket,
                {"status": "error", "message": "Missing transaction ID"},
                client_socket,
            )
            return
        with self.lock:
            client_socket_pending = self.pending_commits.pop(transaction_id, None)
        if client_socket_pending:
            self._send_response(client_socket_pending, {"status": "aborted"})
            self._close_socket(client_socket_pending)
        else:
            self._send_response(
                client_socket, {"status": "error", "message": "Unknown transaction ID"}
            )

    def _process_delivered_messages(self):
        while True:
            message = self.paxos_node.deliver()
            if message and message.get("type") == "commit":
                self._handle_commit(message)

    def _handle_commit(self, data: Dict):
        transaction_id = data.get("transaction_id")
        rs = data.get("rs", [])
        ws = data.get("ws", [])
        commit_status = self.certify_transaction(rs, ws)
        response = {"status": "committed"} if commit_status else {"status": "aborted"}
        if commit_status:
            self.apply_transaction(ws)
        with self.lock:
            client_socket = self.pending_commits.pop(transaction_id, None)
        if client_socket:
            self._send_response(client_socket, response)
            self._close_socket(client_socket)

    def certify_transaction(
        self, rs: List[Tuple[str, int]], ws: List[Tuple[str, Any]]
    ) -> bool:
        with self.lock:
            for item, version in rs:
                current = self.database.get(item)
                current_version = current[1] if current else 0
                if current_version != version:
                    return False
            return True

    def apply_transaction(self, ws: List[Tuple[str, Any]]):
        with self.lock:
            for key, value in ws:
                current = self.database.get(key)
                version = current[1] + 1 if current else 1
                self.database.update(key, (value, version))

    def _send_response(
        self,
        client_socket: socket.socket,
        response: Dict,
        close_after: socket.socket = None,
    ):
        try:
            client_socket.sendall(json.dumps(response).encode())
        except Exception:
            pass
        if close_after:
            self._close_socket(close_after)

    def _close_socket(self, client_socket: socket.socket):
        try:
            client_socket.close()
        except Exception:
            pass

    def stop(self):
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
            self.server_socket = None

import socket
import threading
import json
from typing import Dict, List, Set, Tuple, Any
from Database import Database

import argparse

class Server:
    def __init__(self, host: str, port: int, server_id: int, peers: List[Tuple[str, int]]):
        self.host = host
        self.port = port
        self.id = server_id
        self.db = Database()
        self.last_committed = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((host, port))
        self.peers = peers
        self.lock = threading.Lock()
        
    def start(self):
        self.socket.listen()
        print(f"Server {self.id} listening on {self.host}:{self.port}")
        
        # Accept client connections
        accept_thread = threading.Thread(target=self._accept_connections)
        accept_thread.start()
    
    def _accept_connections(self):
        while True:
            client_socket, addr = self.socket.accept()
            client_thread = threading.Thread(target=self._handle_client, args=(client_socket,))
            client_thread.start()
    
    def _handle_client(self, client_socket: socket.socket):
        while True:
            try:
                data = client_socket.recv(4096).decode()
                if not data:
                    break
                    
                message = json.loads(data)
                msg_type = message["type"]
                
                if msg_type == "read":
                    item = message["item"]
                    value, version = self.db.get(item)
                    response = {
                        "type": "read_response",
                        "value": value,
                        "version": version
                    }
                    client_socket.send(json.dumps(response).encode())
                
                elif msg_type == "commit_request":
                    print(f"Server ID[{self.id}] -> Commit request started!")
                    success = self._certification_test(message["rs"])

                    if success:
                        # Handle atomic broadcast
                        message["type"] = "commit"
                        self._broadcast_commit(message)
                        response = {"type": "commit_response", "result": "commit"}
                    else:
                        response = {"type": "commit_response", "result": "abort"}
                    
                    print(f"Server ID[{self.id}] -> Commit response: {response}")
                    client_socket.send(json.dumps(response).encode())
                elif msg_type == "commit":
                    print(f"Server ID[{self.id}] -> Commit writes requested: {message['ws']}")
                    print(f"Server ID[{self.id}] -> Before Apply Writes: {self.db.data}")
                    self._apply_writes(message["ws"])
                    print(f"Server ID[{self.id}] -> After Apply Writes: {self.db.data}")

            except Exception as e:
                print(f"Error handling client: {e}")
                break
        
        client_socket.close()
    
    def _broadcast_commit(self, message: dict):
        # Implement Best-effort Broadcast (BEB)
        for peer in self.peers:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(peer)
                peer_socket.send(json.dumps(message).encode())
                peer_socket.close()
            except Exception as e:
                print(f"Error broadcasting to peer {peer}: {e}")
    
    def _certification_test(self, rs: Dict[str, Tuple[Any, int]]) -> bool:
        for key, (_, version) in rs.items():
            current_value, current_version = self.db.get(key)
            if current_version > version:
                return False
        return True
    
    def _apply_writes(self, ws: Dict[str, Any]):
        with self.lock:
            self.last_committed += 1
            for key, value in ws.items():
                self.db.set(key, value, self.last_committed)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server-id', type=int, help="id of the server")

    args = parser.parse_args()

    if args.server_id != None:
        server_id = str(args.server_id)

        with open("servers_config.json") as f:
            server_config = json.load(f)
        
        if server_config.get(server_id):
            [host, port] = server_config[server_id]

            total_servers = server_config.keys()

            peers = [tuple(server_config[id]) for id in server_config]

            server = Server(host, port, server_id, peers)

            server.start()

        else:
            print("Server ID not found.")
    else:
        print("Server ID must be declared! Use --help to know more.")
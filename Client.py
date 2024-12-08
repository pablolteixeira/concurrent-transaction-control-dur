import socket
import json
import random
from typing import List, Tuple, Any
from Transaction import Transaction

import argparse

class Client:
    def __init__(self, cid: int, servers: List[Tuple[str, int]]):
        self.cid = cid
        self.tid = 0
        self.current_server = None
        self.servers = servers
    
    def begin_transaction(self) -> Transaction:
        print(f"Client ID[{self.cid}] -> Beginning transaction!")
        self.tid += 1
        self.current_server = random.choice(self.servers)
        print(f"Client ID[{self.cid}] -> Choosen server: {self.current_server[0]}:{self.current_server[1]}.")
        return Transaction(self.cid, self.tid)
    
    def read(self, transaction: Transaction, key: str) -> Any:
        print(f"Client ID[{self.cid}] -> Read Operation: {key}")
        # Check write set first
        if key in transaction.ws:
            print(f"Client ID[{self.cid}] -> Key '{key}' found in client ws. Value is '{transaction.ws[key]}'")
            return transaction.ws[key]
        print(f"Client ID[{self.cid}] -> Key '{key}' not found in client ws.")

        # Read from server
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.connect(self.current_server)
        
        request = {
            "type": "read",
            "item": key,
            "cid": self.cid,
            "tid": transaction.tid
        }
        
        print(f"Client ID[{self.cid}] -> Sending request to the choosen server to search for key '{key}' value.")
        server_socket.send(json.dumps(request).encode())
        response = json.loads(server_socket.recv(4096).decode())
        print(f"Client ID[{self.cid}] -> Response received: {response}")
        server_socket.close()
        
        value, version = response["value"], response["version"]
        transaction.rs[key] = (value, version)
        return value
    
    def write(self, transaction: Transaction, key: str, value: Any):
        print(f"Client ID[{self.cid}] -> Write Operation: {key} = {value}")
        transaction.ws[key] = value
    
    def commit(self, transaction: Transaction) -> bool:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.connect(self.current_server)
        
        request = {
            "type": "commit_request",
            "cid": self.cid,
            "tid": transaction.tid,
            "rs": transaction.rs,
            "ws": transaction.ws
        }
        print(f"Client ID[{self.cid}] -> Commit request sent: {request}")
        server_socket.send(json.dumps(request).encode())
        response = json.loads(server_socket.recv(4096).decode())
        server_socket.close()
        
        print(f"Client ID[{self.cid}] -> Commit result! {response['result'] == 'commit'}")
        return response["result"] == "commit"
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--client-id', type=int, help="id of the client")

    args = parser.parse_args()

    if args.client_id != None:
        client_id = args.client_id

        with open("servers_config.json") as f:
            server_config = json.load(f)
        
        servers = [tuple(server_config[id]) for id in server_config]

        client = Client(client_id, servers)
    else:
        print("Server ID must be declared! Use --help to know more.")
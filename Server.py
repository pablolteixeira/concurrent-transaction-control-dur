import socket
import threading
import json
import time
import copy
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
        
        # Paxos variables
        self.proposal_number = 0
        self.accepted_proposals: Dict[str, Tuple[int, Any]] = {}
        self.promised_proposals: Dict[str, int] = {}
        self.quorum_size = (len(peers) // 2) + 1
        self.pending_proposals: Dict[str, Dict] = {}
        print(f"Server {self.id} initialized with quorum size: {self.quorum_size}")

    def start(self):
        self.socket.listen()
        print(f"DUR Server {self.id} listening on {self.host}:{self.port}")
        
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

                #print(message)

                if msg_type == "read":
                    self._handle_read(client_socket, message)
                elif msg_type == "commit_request":
                    self._handle_commit_request(client_socket, message)
                elif msg_type == "prepare":
                    self._handle_prepare(client_socket, message)
                elif msg_type == "promise":
                    self._handle_promise(client_socket, message)
                elif msg_type == "accept":
                    time.sleep(3)
                    self._handle_accept(client_socket, message)
                elif msg_type == "accepted":
                    self._handle_accepted(client_socket, message)
                elif msg_type == "commit":
                    self._handle_commit(message)

            except Exception as e:
                print(f"Error handling client: {e.with_traceback()}")
                break
        
        client_socket.close()
    
    def _handle_read(self, client_socket: socket.socket, message: Dict):
        print(f"Server ID[{self.id}] -> Read request received for key: {message['item']}")
        item = message["item"]
        value, version = self.db.get(item)
        response = {
            "type": "read_response",
            "value": value,
            "version": version
        }
        client_socket.send(json.dumps(response).encode())
        print(f"Server ID[{self.id}] -> Read request sent: {response}")

    def _handle_commit_request(self, client_socket: socket.socket, message: Dict):
        print(f"\n{'='*50}")
        print(f"Server ID[{self.id}] -> Starting commit request")
        print(f"Read Set: {message['rs']}")
        print(f"Write Set: {message['ws']}")

        read_set = message["rs"]
        write_set = message["ws"]
        
        # First perform certification test
        certification_result = self._certification_test(read_set)
        print(f"Certification test result: {'PASSED' if certification_result else 'FAILED'}")

        if not certification_result:
            response = {"type": "commit_response", "result": "abort"}
            client_socket.send(json.dumps(response).encode())
            print("Transaction aborted due to certification failure")
            return
            
        # Start Paxos consensus for the entire write set
        self.proposal_number += 1
        proposal_id = (self.proposal_number, self.id)
        print(f"Generated new proposal ID: ({proposal_id[0]}, {proposal_id[1]})")

        # Create a unique identifier for this transaction
        tx_id = f"tx_{proposal_id[0]}_{proposal_id[1]}"
        print(f"Created transaction ID: {tx_id}")

        # Store the proposal state
        self.pending_proposals[tx_id] = {
            "proposal_id": proposal_id,
            "client_socket": client_socket,
            "write_set": write_set,
            "promises": set(),
            "accepts": set(),
            "accept_broadcast_sent": False,
            "highest_accepted": None
        }
        print(f"Stored proposal state for transaction {tx_id}")

        # Send prepare messages
        print("Broadcasting prepare messages to peers...")
        self._broadcast_prepare(tx_id, proposal_id, write_set)

    def _handle_prepare(self, client_socket: socket.socket, message: Dict):
        print(f"\n{'='*50}")
        tx_id = message["tx_id"]
        proposal_id = (message["proposal_number"], message["proposer_id"])
        print(f"Server ID[{self.id}] -> Received PREPARE for tx {tx_id}")
        print(f"Proposal ID: {proposal_id}")
        
        with self.lock:
            current_promised = self.promised_proposals.get(tx_id, 0)
            print(f"Current promised proposal number: {current_promised}")
            print(f"Received proposal number: {proposal_id[0]}")
            
            if proposal_id[0] > current_promised:
                self.promised_proposals[tx_id] = proposal_id[0]
                print(f"Promise accepted. Updated promised proposal to: {proposal_id[0]}")
                
                response = {
                    "type": "promise",
                    "tx_id": tx_id,
                    "proposal_number": proposal_id[0],
                    "proposer_id": proposal_id[1],
                    "accepted_proposal": self.accepted_proposals.get(tx_id),
                    "acceptor_id": self.id
                }
                
                proposer_address = message["proposer_address"]

                prepare_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                prepare_socket.connect((proposer_address["host"], proposer_address["port"]))
                prepare_socket.send(json.dumps(response).encode())
                prepare_socket.close()
                print(f"Sent PROMISE response for tx {tx_id}")
            else:
                print(f"Promise rejected. Current promised ({current_promised}) >= received ({proposal_id[0]})")
    
    def _handle_local_prepare(self, message: Dict):
        """Handle prepare message locally without socket communication"""
        print(f"\n{'='*50}")
        tx_id = message["tx_id"]
        proposal_id = (message["proposal_number"], message["proposer_id"])
        print(f"Server ID[{self.id}] -> Handling local PREPARE for tx {tx_id}")
        
        with self.lock:
            current_promised = self.promised_proposals.get(tx_id, 0)
            print(f"Current promised proposal number: {current_promised}")
            print(f"Received proposal number: {proposal_id[0]}")
            
            if proposal_id[0] > current_promised:
                self.promised_proposals[tx_id] = proposal_id[0]
                print(f"Local promise accepted. Updated promised proposal to: {proposal_id[0]}")

                self.lock.release()
                try:
                    # Create promise message
                    promise_msg = {
                        "type": "promise",
                        "tx_id": tx_id,
                        "proposal_number": proposal_id[0],
                        "proposer_id": proposal_id[1],
                        "accepted_proposal": self.accepted_proposals.get(tx_id),
                        "acceptor_id": self.id
                    }
                    
                    # Handle the promise message directly
                    self._handle_promise(None, promise_msg)
                finally:
                    # Reacquire lock since we're in a with block
                    self.lock.acquire()
            else:
                print(f"Local promise rejected. Current promised ({current_promised}) >= received ({proposal_id[0]})")

    def _handle_promise(self, client_socket: socket.socket, message: Dict):
        print(f"\n{'='*50}")
        tx_id = message["tx_id"]
        print(f"Server ID[{self.id}] -> Received PROMISE for tx {tx_id}")
        print(f"Promise from acceptor: {message['acceptor_id']}")
        
        with self.lock:
            if tx_id in self.pending_proposals:
                proposal = self.pending_proposals[tx_id]
                proposal["promises"].add(message["acceptor_id"])
                print(f"Current promises count: {len(proposal['promises'])}/{self.quorum_size}")
                print(f"Promises from: {proposal['promises']}")
                
                if message["accepted_proposal"]:
                    print(f"Received previously accepted proposal: {message['accepted_proposal']}")
                    if (not proposal["highest_accepted"] or 
                        message["accepted_proposal"][0] > proposal["highest_accepted"][0]):
                        proposal["highest_accepted"] = message["accepted_proposal"]
                        print(f"Updated highest accepted proposal to: {proposal['highest_accepted']}")
                
                if len(proposal["promises"]) >= self.quorum_size and not proposal["accept_broadcast_sent"]:
                    print(f"Quorum achieved for PROMISE phase! ({len(proposal['promises'])}/{self.quorum_size})")
                    accept_value = (proposal["highest_accepted"][1] if proposal["highest_accepted"] 
                                else proposal["write_set"])

                    proposal["accept_broadcast_sent"] = True

                    self.lock.release()
                    try:
                        print("Broadcasting ACCEPT messages...")
                        self._broadcast_accept(tx_id, proposal["proposal_id"][0], accept_value)
                    finally:
                        self.lock.acquire()
            else:
                print(f"Warning: Received promise for unknown transaction {tx_id}")

    def _handle_accept(self, client_socket: socket.socket, message: Dict):
        print(f"\n{'='*50}")
        tx_id = message["tx_id"]
        proposal_number = message["proposal_number"]
        print(f"Server ID[{self.id}] -> Received ACCEPT for tx {tx_id}")
        print(f"Proposal number: {proposal_number}")
        
        with self.lock:
            current_promised = self.promised_proposals.get(tx_id, 0)
            print(f"Current promised number: {current_promised}")
            
            if proposal_number >= current_promised:
                print("Accept request is valid")
                self.accepted_proposals[tx_id] = (proposal_number, message["write_set"])
                response = {
                    "type": "accepted",
                    "tx_id": tx_id,
                    "proposal_number": proposal_number,
                    "acceptor_id": self.id
                }

                accept_address = message["proposer_address"]

                accept_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                accept_socket.connect((accept_address["host"], accept_address["port"]))
                accept_socket.send(json.dumps(response).encode())
                accept_socket.close()
                print(f"Sent ACCEPTED response for tx {tx_id}")
            else:
                print(f"Accept rejected. Current promised ({current_promised}) > received ({proposal_number})")

    def _handle_local_accept(self, message: Dict):
        """Handle accept message locally without socket communication"""
        print(f"\n{'='*50}")
        tx_id = message["tx_id"]
        proposal_number = message["proposal_number"]
        print(f"Server ID[{self.id}] -> Handling local ACCEPT for tx {tx_id}")
        
        with self.lock:
            if proposal_number >= self.promised_proposals.get(tx_id, 0):
                print("Local accept is valid")
                self.accepted_proposals[tx_id] = (proposal_number, message["write_set"])
                
                self.lock.release()
                try:
                    # Create accepted message and handle it directly
                    accepted_msg = {
                        "type": "accepted",
                        "tx_id": tx_id,
                        "proposal_number": proposal_number,
                        "acceptor_id": self.id
                    }
                    # Handle the accepted message directly
                    self._handle_accepted(None, accepted_msg)
                finally:
                    # Make sure we reacquire the lock since we're in a with block
                    self.lock.acquire()
            else:
                print(f"Local accept rejected. Current promised > received proposal")

    def _handle_accepted(self, client_socket: socket.socket, message: Dict):
        tx_id = message["tx_id"]
        
        with self.lock:
            if self.pending_proposals.get(tx_id):
                proposal = self.pending_proposals[tx_id]
                proposal["accepts"].add(message["acceptor_id"])
                
                if len(proposal["accepts"]) >= self.quorum_size:
                    # We have consensus! Broadcast the commit
                    write_set = proposal["write_set"]
                    del self.pending_proposals[tx_id]

                    commit_message = {
                        "type": "commit",
                        "write_set": write_set
                    }
                    self.lock.release()
                    try:
                        # We have consensus! Broadcast the commit
                        commit_message = {
                            "type": "commit",
                            "write_set": write_set
                        }
                        self._broadcast_commit(commit_message)
                    finally:
                        # Make sure we reacquire the lock since we're in a with block
                        self.lock.acquire()

                    time.sleep(1)
                    # Respond to the client
                    response = {"type": "commit_response", "result": "commit"}
                    proposal["client_socket"].send(json.dumps(response).encode())
                    

    def _handle_commit(self, message: Dict):
        print(f"\n{'='*50}")
        write_set = message["write_set"]
        print(f"Server ID[{self.id}] -> Committing write set")
        print(f"Write set: {write_set}")
        
        print("Current database state:", self.db.data)
        self._apply_writes(write_set)
        print("New database state:", self.db.data)

    def _certification_test(self, rs: Dict[str, Tuple[Any, int]]) -> bool:
        print(f"Running certification test for read set: {rs}")
        for key, (_, version) in rs.items():
            _, current_version = self.db.get(key)
            print(f"Key: {key}, Read version: {version}, Current version: {current_version}")
            if current_version > version:
                print(f"Certification failed for key {key}")
                return False
        print("Certification test passed")
        return True
    
    def _apply_writes(self, ws: Dict[str, Any]):
        with self.lock:
            self.last_committed += 1
            for key, value in ws.items():
                self.db.set(key, value, self.last_committed)

    def _broadcast_prepare(self, tx_id: str, proposal_id: Tuple[int, int], write_set: Dict):
        message = {
            "type": "prepare",
            "tx_id": tx_id,
            "proposal_number": proposal_id[0],
            "proposer_id": proposal_id[1],
            "write_set": write_set,
            "proposer_address": {
                "host": self.host,
                "port": self.port
            }
        }
        print(f"Broadcasting PREPARE message for tx {tx_id}")
        print(f"Message content: {message}")

        # Handle local prepare first
        self._handle_local_prepare(message)

        # Then broadcast to other peers
        self._broadcast_to_peers(message)

    def _broadcast_accept(self, tx_id: str, proposal_number: int, write_set: Dict):
        message = {
            "type": "accept",
            "tx_id": tx_id,
            "proposal_number": proposal_number,
            "write_set": write_set,
            "proposer_address": {
                "host": self.host,
                "port": self.port
            }
        }
        print(f"Broadcasting ACCEPT message for tx {tx_id}")
        print(f"Message content: {message}")
        
        # Handle local accept first
        self._handle_local_accept(message)
        
        # Then broadcast to other peers
        self._broadcast_to_peers(message)

    def _broadcast_commit(self, message: Dict):
        print("Broadcasting COMMIT message")
        print(f"Message content: {message}")
        
        # Handle local commit first
        self._handle_commit(message)
        
        # Then broadcast to other peers
        self._broadcast_to_peers(message)

    def _broadcast_to_peers(self, message: Dict):
        print(f"Broadcasting to peers: {self.peers}")
        for peer in self.peers:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(peer)
                peer_socket.send(json.dumps(message).encode())
                peer_socket.close()
                print(f"Successfully sent message to peer {peer}")
            except Exception as e:
                print(f"Error broadcasting to peer {peer}: {e}")

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

            peers = [tuple(server_config[id]) for id in server_config if server_config[id] != server_config[server_id]]

            server = Server(host, port, server_id, peers)

            server.start()

        else:
            print("Server ID not found.")
    else:
        print("Server ID must be declared! Use --help to know more.")
import socket
import json
import threading
from typing import List, Dict, Tuple
from queue import PriorityQueue


class PaxosNode:
    """
    Represents a single Paxos node for atomic broadcast.
    """

    def __init__(
        self, node_id: int, server_addresses: List[Tuple[str, int]], listen_port: int
    ):
        self.node_id = node_id
        self.server_addresses = server_addresses
        self.listen_port = listen_port
        self.lock = threading.Lock()

        # Paxos State
        self.proposals = {}
        self.accepted_proposals = {}
        self.promised_id = None
        self.accepted_id = None
        self.accepted_value = None

        # Message Queue for Delivery
        self.message_queue = PriorityQueue()

    def start(self):
        listener_thread = threading.Thread(target=self._listen)
        listener_thread.daemon = True
        listener_thread.start()
        print(f"Paxos Node {self.node_id} listening on port {self.listen_port}")

    def propose(self, message: Dict):
        proposal_id = self._generate_proposal_id()
        self.proposals[proposal_id] = message
        self._send_to_all(
            {"type": "prepare", "proposal_id": proposal_id, "node_id": self.node_id}
        )

    def _handle_prepare(self, data: Dict, address: Tuple[str, int]):
        proposal_id = data["proposal_id"]

        with self.lock:
            if not self.promised_id or proposal_id > self.promised_id:
                self.promised_id = proposal_id
                response = {
                    "type": "promise",
                    "proposal_id": proposal_id,
                    "accepted_id": self.accepted_id,
                    "accepted_value": self.accepted_value,
                }
                self._send_message(address, response)

    def _handle_promise(self, data: Dict):
        proposal_id = data["proposal_id"]

        with self.lock:
            if proposal_id in self.proposals:
                if not hasattr(self, "_promise_count"):
                    self._promise_count = {}
                self._promise_count[proposal_id] = (
                    self._promise_count.get(proposal_id, 0) + 1
                )
                if self._promise_count[proposal_id] > len(self.server_addresses) // 2:
                    message = self.proposals[proposal_id]
                    self._send_to_all(
                        {
                            "type": "accept",
                            "proposal_id": proposal_id,
                            "message": message,
                        }
                    )

    def _handle_accept(self, data: Dict, address: Tuple[str, int]):
        proposal_id = data["proposal_id"]
        message = data["message"]

        with self.lock:
            if not self.promised_id or proposal_id >= self.promised_id:
                self.accepted_id = proposal_id
                self.accepted_value = message
                response = {"type": "accepted", "proposal_id": proposal_id}
                self._send_message(address, response)

    def _handle_accepted(self, data: Dict):
        proposal_id = data["proposal_id"]

        with self.lock:
            if proposal_id in self.proposals:
                if not hasattr(self, "_accept_count"):
                    self._accept_count = {}
                self._accept_count[proposal_id] = (
                    self._accept_count.get(proposal_id, 0) + 1
                )
                if self._accept_count[proposal_id] > len(self.server_addresses) // 2:
                    message = self.proposals.pop(proposal_id)
                    self.message_queue.put((proposal_id, message))

    def deliver(self) -> Dict:
        """
        Delivers the next message in total order.
        Returns:
            Dict: The next delivered message.
        """
        _, message = self.message_queue.get()
        return message

    def _listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", self.listen_port))

        while True:
            try:
                data, address = sock.recvfrom(4096)
                message = json.loads(data.decode())

                if message["type"] == "prepare":
                    self._handle_prepare(message, address)
                elif message["type"] == "promise":
                    self._handle_promise(message)
                elif message["type"] == "accept":
                    self._handle_accept(message, address)
                elif message["type"] == "accepted":
                    self._handle_accepted(message)

            except Exception as e:
                print(f"Error in Paxos node: {e}")

    def _send_to_all(self, message: Dict):
        for address in self.server_addresses:
            self._send_message(address, message)

    def _send_message(self, address: Tuple[str, int], message: Dict):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(json.dumps(message).encode(), (address[0], address[1]))
        finally:
            sock.close()

    def _generate_proposal_id(self) -> int:
        with self.lock:
            if not hasattr(self, "_proposal_counter"):
                self._proposal_counter = 0
            self._proposal_counter += 1
            return self._proposal_counter

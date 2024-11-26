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

        self.proposals = {}
        self.accepted_proposals = {}
        self.promised_id = None
        self.accepted_id = None
        self.accepted_value = None

        self.message_queue = PriorityQueue()

        self._promise_count = {}
        self._accept_count = {}
        self._proposal_counter = 0

    def start(self):
        listener_thread = threading.Thread(target=self._listen, daemon=True)
        listener_thread.start()
        print(f"Paxos Node {self.node_id} listening on port {self.listen_port}")

    def propose(self, message: Dict):
        proposal_id = self._generate_proposal_id()
        with self.lock:
            self.proposals[proposal_id] = message
        self._send_to_all(
            {"type": "prepare", "proposal_id": proposal_id, "node_id": self.node_id}
        )

    def _handle_prepare(self, data: Dict, address: Tuple[str, int]):
        proposal_id = data["proposal_id"]

        with self.lock:
            if self.promised_id is None or proposal_id > self.promised_id:
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
            if self.promised_id is None or proposal_id >= self.promised_id:
                self.accepted_id = proposal_id
                self.accepted_value = message
                response = {"type": "accepted", "proposal_id": proposal_id}
                self._send_message(address, response)

    def _handle_accepted(self, data: Dict):
        proposal_id = data["proposal_id"]

        with self.lock:
            if proposal_id in self.proposals:
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
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(("", self.listen_port))
            print(f"Node {self.node_id} is ready to receive messages.")

            while True:
                try:
                    data, address = sock.recvfrom(4096)
                    message = json.loads(data.decode())
                    self._dispatch_message(message, address)
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON received by Node {self.node_id}: {e}")
                except Exception as e:
                    print(f"Error in Paxos node {self.node_id}: {e}")

    def _dispatch_message(self, message: Dict, address: Tuple[str, int]):
        try:
            handlers = {
                "prepare": self._handle_prepare,
                "promise": self._handle_promise,
                "accept": self._handle_accept,
                "accepted": self._handle_accepted,
            }
            handler = handlers.get(message["type"])
            if handler:
                handler(message, address)
            else:
                print(f"Unknown message type received: {message.get('type')}")
        except Exception as e:
            print(f"Error handling message {message} from {address}: {e}")

    def _send_to_all(self, message: Dict):
        for address in self.server_addresses:
            self._send_message(address, message)

    def _send_message(self, address: Tuple[str, int], message: Dict):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.sendto(json.dumps(message).encode(), address)
        except Exception as e:
            print(f"Error sending message to {address}: {e}")

    def _generate_proposal_id(self) -> int:
        with self.lock:
            self._proposal_counter += 1
            return (self._proposal_counter << 16) | self.node_id

import socket
import json
import threading
import logging
from typing import List, Dict, Tuple
from queue import PriorityQueue, Empty

logger = logging.getLogger("PaxosNode")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s"
    )
)
if not logger.handlers:
    logger.addHandler(ch)


class PaxosNode:
    def __init__(
        self, node_id: int, server_addresses: List[Tuple[str, int]], listen_port: int
    ):
        self.node_id = node_id
        self.server_addresses = server_addresses
        self.listen_port = listen_port
        self.lock = threading.Lock()
        self.proposals = {}
        self.promised_id = None
        self.accepted_id = None
        self.accepted_value = None
        self.message_queue = PriorityQueue()
        self._promise_count = {}
        self._accept_count = {}
        self._proposal_counter = 0

    def start(self):
        threading.Thread(
            target=self._listen, name=f"PaxosListener-{self.node_id}", daemon=True
        ).start()

    def propose(self, message: Dict):
        proposal_id = self._generate_proposal_id()
        with self.lock:
            self.proposals[proposal_id] = message
        self._send_to_all(
            {"type": "prepare", "proposal_id": proposal_id, "node_id": self.node_id}
        )

    def deliver(self) -> Dict:
        try:
            return self.message_queue.get(timeout=10)[1]
        except Empty:
            return {}

    def _listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(("", self.listen_port))
            while True:
                try:
                    data, address = sock.recvfrom(4096)
                    message = json.loads(data.decode())
                    self._dispatch_message(message, address)
                except json.JSONDecodeError:
                    pass

    def _dispatch_message(self, message: Dict, address: Tuple[str, int]):
        {
            "prepare": self._handle_prepare,
            "promise": self._handle_promise,
            "accept": self._handle_accept,
            "accepted": self._handle_accepted,
        }.get(message.get("type"), lambda *_: None)(message, address)

    def _handle_prepare(self, data: Dict, address: Tuple[str, int]):
        with self.lock:
            if self.promised_id is None or data["proposal_id"] > self.promised_id:
                self.promised_id = data["proposal_id"]
                self._send_message(
                    address,
                    {
                        "type": "promise",
                        "proposal_id": data["proposal_id"],
                        "node_id": self.node_id,
                        "accepted_id": self.accepted_id,
                        "accepted_value": self.accepted_value,
                    },
                )

    def _handle_promise(self, data: Dict, address: Tuple[str, int]):
        with self.lock:
            proposal_id = data["proposal_id"]
            if proposal_id in self.proposals:
                self._promise_count[proposal_id] = (
                    self._promise_count.get(proposal_id, 0) + 1
                )
                if self._promise_count[proposal_id] > len(self.server_addresses) // 2:
                    self._send_to_all(
                        {
                            "type": "accept",
                            "proposal_id": proposal_id,
                            "message": self.proposals[proposal_id],
                            "node_id": self.node_id,
                        }
                    )

    def _handle_accept(self, data: Dict, address: Tuple[str, int]):
        with self.lock:
            if self.promised_id is None or data["proposal_id"] >= self.promised_id:
                self.promised_id = data["proposal_id"]
                self.accepted_id = data["proposal_id"]
                self.accepted_value = data["message"]
                self._send_message(
                    address,
                    {
                        "type": "accepted",
                        "proposal_id": data["proposal_id"],
                        "node_id": self.node_id,
                    },
                )

    def _handle_accepted(self, data: Dict, address: Tuple[str, int]):
        with self.lock:
            proposal_id = data["proposal_id"]
            if proposal_id in self.proposals:
                self._accept_count[proposal_id] = (
                    self._accept_count.get(proposal_id, 0) + 1
                )
                if self._accept_count[proposal_id] > len(self.server_addresses) // 2:
                    self.message_queue.put(
                        (proposal_id, self.proposals.pop(proposal_id))
                    )

    def _send_to_all(self, message: Dict):
        for address in self.server_addresses:
            if address != ("localhost", self.listen_port):
                self._send_message(address, message)

    def _send_message(self, address: Tuple[str, int], message: Dict):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.sendto(json.dumps(message).encode(), address)
        except Exception as e:
            logger.error("Send error: %s", e)

    def _generate_proposal_id(self) -> int:
        with self.lock:
            self._proposal_counter += 1
            return (self._proposal_counter << 16) | self.node_id

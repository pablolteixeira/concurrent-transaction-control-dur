import threading
from ReplicationServer import ReplicationServer
from TransactionClient import TransactionClient
import time


def start_server(node_id, host, port, paxos_port, server_addresses):
    server = ReplicationServer(host, port, node_id, server_addresses, paxos_port)
    server.start()


def client_thread(server_address):
    client = TransactionClient(server_address)
    client.start_transaction()
    # Simulate transaction operations
    value = client.read("x")
    print(f"Client read x: {value}")
    client.write("x", "new_value")
    if client.commit():
        print("Transaction committed")
    else:
        print("Transaction aborted")
    client.close()


if __name__ == "__main__":
    # Define server addresses
    server_addresses = [
        ("localhost", 10000),  # Server 0
        ("localhost", 10001),  # Server 1
        ("localhost", 10002),  # Server 2
    ]
    paxos_ports = [20000, 20001, 20002]

    # Start servers
    for i in range(len(server_addresses)):
        host, port = server_addresses[i]
        paxos_port = paxos_ports[i]
        threading.Thread(
            target=start_server,
            args=(
                i,
                host,
                port,
                paxos_port,
                list(zip([addr[0] for addr in server_addresses], paxos_ports)),
            ),
            daemon=True,
        ).start()

    # Wait for servers to start
    time.sleep(2)

    # Start client
    threading.Thread(target=client_thread, args=(server_addresses[0],)).start()

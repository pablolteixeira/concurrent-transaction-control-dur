import threading
import time
from typing import List, Tuple
from ReplicationServer import ReplicationServer
from TransactionClient import TransactionClient


def main():
    # Define server addresses for Paxos (only one server in this test)
    server_paxos_port = 11000
    server_client_port = 10000
    server_addresses: List[Tuple[str, int]] = [("localhost", server_paxos_port)]

    # Initialize and start the ReplicationServer
    print("Initializing ReplicationServer...")
    server = ReplicationServer(
        host="localhost",
        port=server_client_port,
        node_id=1,
        server_addresses=server_addresses,
        paxos_port=server_paxos_port,
    )
    server_thread = threading.Thread(
        target=server.start, daemon=True, name="ReplicationServer-1"
    )
    server_thread.start()
    print("ReplicationServer started and listening for connections.")

    # Allow the server some time to start
    time.sleep(2)

    # Initialize the TransactionClient
    print("\nInitializing TransactionClient...")
    client = TransactionClient(server_address=("localhost", server_client_port))
    print("TransactionClient connected to ReplicationServer.")

    # Start a new transaction
    print("\nStarting a new transaction...")
    client.start_transaction()
    print(f"Transaction started with ID: {client.transaction_id}")

    # Perform write operations
    print("\nPerforming write operations...")
    client.write("x", 10)
    print("Wrote key 'x' with value 10.")
    client.write("y", 20)
    print("Wrote key 'y' with value 20.")

    # Commit the transaction
    print("\nCommitting the transaction...")
    try:
        commit_success = client.commit()
        print(f"Commit successful: {commit_success}")
    except Exception as e:
        print(f"Commit failed: {e}")

    # Start another transaction to read the values
    print("\nStarting a new transaction for reading...")
    client.start_transaction()
    print(f"Transaction started with ID: {client.transaction_id}")

    # Perform read operations
    print("\nPerforming read operations...")
    try:
        value_x = client.read("x")
        print(f"Read key 'x': {value_x}")
        value_y = client.read("y")
        print(f"Read key 'y': {value_y}")
    except Exception as e:
        print(f"Read failed: {e}")

    # Close the client connection
    print("\nClosing TransactionClient connection...")
    client.close()
    print("TransactionClient connection closed.")

    # Allow some time for all logs to be printed before shutting down
    time.sleep(2)

    # Stop the ReplicationServer
    print("\nStopping ReplicationServer...")
    server.stop()
    print("ReplicationServer stopped.")


if __name__ == "__main__":
    main()

import time
import threading
from Server import Server
from Client import Client

# Test case implementation
def run_test_scenario():
    # Set up servers
    servers = [
        Server("localhost", 5000, 0, 3),
        Server("localhost", 5001, 1, 3),
        Server("localhost", 5002, 2, 3)
    ]
    
    # Set up peer information
    server_addresses = [("localhost", 5000), ("localhost", 5001), ("localhost", 5002)]
    for server in servers:
        server.peers = [addr for addr in server_addresses if addr != (server.host, server.port)]
        server.start()
    
    # Set up clients
    client1 = Client(1)
    client2 = Client(2)
    client1.servers = server_addresses
    client2.servers = server_addresses
    
    # Test scenario with concurrent transactions
    def client1_transaction():
        tx = client1.begin_transaction()
        client1.write(tx, "x", 10)
        time.sleep(0.1)  # Force concurrency
        value = client1.read(tx, "y")
        client1.write(tx, "y", value + 5 if value else 5)
        return client1.commit(tx)
    
    def client2_transaction():
        tx = client2.begin_transaction()
        client2.write(tx, "y", 20)
        time.sleep(0.1)  # Force concurrency
        value = client2.read(tx, "x")
        client2.write(tx, "x", value + 10 if value else 10)
        return client2.commit(tx)
    
    # Run concurrent transactions
    thread1 = threading.Thread(target=client1_transaction)
    thread2 = threading.Thread(target=client2_transaction)
    
    thread1.start()
    thread2.start()
    
    thread1.join()
    thread2.join()

if __name__ == "__main__":
    run_test_scenario()
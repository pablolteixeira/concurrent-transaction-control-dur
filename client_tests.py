import json
import time
import threading
from Client import Client

if __name__ == "__main__":
    with open("servers_config.json") as f:
        server_config = json.load(f)
    
    servers = [tuple(server_config[id]) for id in server_config]
    client1 = Client(1, servers)
    #client2 = Client(2, servers)

    # Test scenario with concurrent transactions
    def client1_transaction():
        tx1 = client1.begin_transaction()
        #tx2 = client2.begin_transaction()
        client1.write(tx1, "x", 10)
        #value2r = client2.read(tx2, "x")
        value = client1.read(tx1, "y")
        #client2.write(tx2, "x", value2r + 10 if value2r else 10)
        client1.write(tx1, "y", value + 5 if value else 5)
        return client1.commit(tx1)
    """
    def client2_transaction():
        tx = client2.begin_transaction()
        client2.write(tx, "y", 20)
        time.sleep(0.1)  # Force concurrency
        value = client2.read(tx, "x")
        client2.write(tx, "x", value + 10 if value else 10)
        return client2.commit(tx)
    """
    # Run concurrent transactions
    thread1 = threading.Thread(target=client1_transaction)
    #thread2 = threading.Thread(target=client2_transaction)
    
    thread1.start()
    #thread2.start()
    
    thread1.join()
    #thread2.join()
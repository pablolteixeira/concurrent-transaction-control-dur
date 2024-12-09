import json
import time
import threading
from Client import Client


def client1_transaction(client, servers):
    """
    # Transação do cliente 1
    ## Entrada esperada:
    - Transação que escreve em "x" e "y".
    - Valor inicial de "x" e "y" inexistente no banco de dados.
    ## Comportamento esperado:
    - O cliente 1 deve escrever 10 em "x".
    - Ler o valor atual de "y" (esperado como None).
    - Escrever 5 em "y" (baseado em valor lido como None + 5).
    - Commit deve ser bem-sucedido.
    ## Saída esperada:
    - Commit bem-sucedido e banco atualizado com {"x": 10, "y": 5}.
    """
    tx1 = client.begin_transaction()
    client.write(tx1, "x", 10)
    value = client.read(tx1, "y")
    client.write(tx1, "y", value + 5 if value else 5)
    result = client.commit(tx1)
    print(f"Resultado do commit do cliente 1: {result}")
    return result


def client2_transaction(client, servers):
    """
    # Transação do cliente 2
    ## Entrada esperada:
    - Transação concorrente que escreve em "y" e "x".
    - Valor inicial de "y" e "x" existente no banco de dados devido à transação do cliente 1.
    ## Comportamento esperado:
    - O cliente 2 deve escrever 20 em "y".
    - Ler o valor atual de "x" (esperado como 10).
    - Escrever 20 em "x" (baseado em valor lido + 10).
    - Commit deve ser bem-sucedido.
    ## Saída esperada:
    - Commit bem-sucedido e banco atualizado com {"x": 20, "y": 20}.
    """
    tx2 = client.begin_transaction()
    client.write(tx2, "y", 20)
    time.sleep(0.1)  # Força concorrência
    value = client.read(tx2, "x")
    client.write(tx2, "x", value + 10 if value else 10)
    result = client.commit(tx2)
    print(f"Resultado do commit do cliente 2: {result}")
    return result


def validate_state(client, expected_state):
    """
    Valida o estado atual do banco de dados contra o estado esperado.
    """
    tx = client.begin_transaction()
    for key, expected_value in expected_state.items():
        actual_value = client.read(tx, key)
        assert (
            actual_value == expected_value
        ), f"Falha: {key} esperado {expected_value}, encontrado {actual_value}"
    client.commit(tx)


def main():
    with open("servers_config.json") as f:
        server_config = json.load(f)

    servers = [tuple(server_config[id]) for id in server_config]
    client1 = Client(1, servers)
    client2 = Client(2, servers)

    # Teste 1: Transações sequenciais
    """
    # Cenário de teste 1: Transações sequenciais
    ## Comportamento esperado:
    - Cliente 1 realiza uma transação que escreve valores iniciais.
    - Cliente 2 realiza uma transação que atualiza os valores baseados nos escritos do cliente 1.
    ## Saída esperada:
    - Banco atualizado com {"x": 20, "y": 20}.
    """
    client1_transaction(client1, servers)
    client2_transaction(client2, servers)
    validate_state(client1, {"x": 20, "y": 20})
    print("Teste 1 passou!")

    # Teste 2: Transações concorrentes
    """
    # Cenário de teste 2: Transações concorrentes
    ## Comportamento esperado:
    - Cliente 1 e cliente 2 iniciam transações ao mesmo tempo.
    - Paxos deve garantir consistência e commits corretos.
    ## Saída esperada:
    - Banco consistente após ambas as transações, estado final pode variar, mas deve refletir operações válidas.
    """
    thread1 = threading.Thread(target=client1_transaction, args=(client1, servers))
    thread2 = threading.Thread(target=client2_transaction, args=(client2, servers))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    # Estado esperado após transações concorrentes depende da ordem de commit
    validate_state(client1, {"x": 30, "y": 20})
    print("Teste 2 passou!")

    # Teste 3: Leitura após escrita
    """
    # Cenário de teste 3: Leitura após escrita
    ## Comportamento esperado:
    - Cliente 1 realiza uma escrita em "z".
    - Cliente 2 lê o valor de "z" e verifica se está consistente.
    ## Saída esperada:
    - Cliente 2 lê o valor que o cliente 1 escreveu.
    """
    tx1 = client1.begin_transaction()
    client1.write(tx1, "z", 50)
    client1.commit(tx1)

    tx2 = client2.begin_transaction()
    value = client2.read(tx2, "z")
    assert value == 50, f"Falha: esperado 50, encontrado {value}"
    client2.commit(tx2)
    print("Teste 3 passou!")


if __name__ == "__main__":
    main()

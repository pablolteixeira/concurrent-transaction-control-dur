import socket
import threading
import json
import time
from typing import Dict, Tuple
import os


# Helper function to start a server in a separate thread
def start_server(server_id: int):
    os.system(f"python3 Server.py -s {server_id}")


# Helper function to send messages to a server
def send_message(host: str, port: int, message: Dict) -> Dict:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        client_socket.send(json.dumps(message).encode())
        response = client_socket.recv(4096).decode()
        return json.loads(response)


# Test Cases
def test_read_operation():
    """
    # Teste de operação de leitura
    ## Entrada esperada:
    - Solicitação de leitura para uma chave que existe no banco de dados.
    ## Comportamento esperado:
    - O servidor retorna o valor associado à chave e sua versão.
    ## Saída esperada:
    - Resposta no formato {"type": "read_response", "value": "valor", "version": int}.
    """
    response = send_message("localhost", 8001, {"type": "read", "item": "chave1"})
    assert response["type"] == "read_response", "Resposta incorreta do servidor"
    print("Teste de operação de leitura passou!")


def test_commit_request_success():
    """
    # Teste de solicitação de commit com sucesso
    ## Entrada esperada:
    - Um conjunto de leitura consistente com o estado atual do banco de dados.
    - Um conjunto de escrita para atualização.
    ## Comportamento esperado:
    - O servidor passa no teste de certificação, inicia o consenso Paxos e retorna commit.
    ## Saída esperada:
    - Resposta no formato {"type": "commit_response", "result": "commit"}.
    """
    message = {
        "type": "commit_request",
        "rs": {"chave1": ("valor1", 1)},
        "ws": {"chave2": "valor2"},
    }
    response = send_message("localhost", 8001, message)
    assert response["type"] == "commit_response", "Resposta incorreta do servidor"
    assert response["result"] == "commit", "Commit falhou inesperadamente"
    print("Teste de solicitação de commit com sucesso passou!")


def test_commit_request_abort():
    """
    # Teste de solicitação de commit abortado
    ## Entrada esperada:
    - Um conjunto de leitura inconsistente com o estado atual do banco de dados.
    - O valor de 'chave1' é manualmente configurado para causar conflito.
    ## Comportamento esperado:
    - O servidor falha no teste de certificação e retorna abort.
    ## Saída esperada:
    - Resposta no formato {"type": "commit_response", "result": "abort"}.
    """

    # Passo 1: Definir manualmente o valor no banco de dados
    setup_message = {
        "type": "commit_request",
        "rs": {},  # Nenhum conjunto de leitura, apenas escrita para setup
        "ws": {"chave1": "valor_atualizado"},
    }
    setup_response = send_message("localhost", 8001, setup_message)
    assert setup_response["type"] == "commit_response", "Erro no setup inicial"
    assert setup_response["result"] == "commit", "Setup falhou inesperadamente"

    # Passo 2: Iniciar o teste com um conjunto de leitura inconsistente
    test_message = {
        "type": "commit_request",
        "rs": {"chave1": ("valor1", 1)},  # Valor e versão desatualizados
        "ws": {"chave3": "valor3"},
    }
    response = send_message("localhost", 8001, test_message)

    # Passo 3: Validar resposta de abort
    assert response["type"] == "commit_response", "Resposta incorreta do servidor"
    assert response["result"] == "abort", "Abort esperado, mas commit ocorreu"
    print("Teste de solicitação de commit abortado passou!")


def test_paxos_consensus():
    """
    # Teste de consenso Paxos
    ## Entrada esperada:
    - Proposta de escrita para consenso em um cluster de servidores.
    ## Comportamento esperado:
    - Um servidor inicia a fase de preparação, coleta promessas, realiza aceitação e comita o conjunto.
    ## Saída esperada:
    - Mensagens de confirmação de consenso são enviadas e o commit ocorre.
    """
    message = {
        "type": "commit_request",
        "rs": {"chave4": ("valor4", 1)},
        "ws": {"chave5": "valor5"},
    }
    response = send_message("localhost", 8001, message)
    assert response["type"] == "commit_response", "Resposta incorreta do servidor"
    assert response["result"] == "commit", "Consenso Paxos falhou"
    print("Teste de consenso Paxos passou!")


# Starting servers for testing
def main():
    """
    Inicia servidores em threads para testes.
    """
    threading.Thread(target=start_server, args=(1,)).start()
    time.sleep(2)

    threading.Thread(target=start_server, args=(2,)).start()
    time.sleep(2)

    threading.Thread(target=start_server, args=(3,)).start()

    time.sleep(5)  # Aguarda os servidores iniciarem

    # Executa os testes
    test_read_operation()
    test_commit_request_success()
    test_commit_request_abort()
    test_paxos_consensus()


if __name__ == "__main__":
    main()

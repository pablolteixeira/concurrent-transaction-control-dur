import socket
import json
import random
from typing import List, Tuple, Any
from Transaction import Transaction

import argparse


class Client:
    def __init__(self, cid: int, servers: List[Tuple[str, int]]):
        self.cid = cid
        self.tid = 0
        self.current_server = None
        self.servers = servers

    def begin_transaction(self) -> Transaction:
        print(f"Cliente ID[{self.cid}] -> Iniciando transação!")
        self.tid += 1
        self.current_server = random.choice(self.servers)
        print(
            f"Cliente ID[{self.cid}] -> Servidor escolhido: {self.current_server[0]}:{self.current_server[1]}."
        )
        return Transaction(self.cid, self.tid)

    def read(self, transaction: Transaction, key: str) -> Any:
        print(f"Cliente ID[{self.cid}] -> Operação de leitura: {key}")
        # Verifica o conjunto de escrita primeiro
        if key in transaction.ws:
            print(
                f"Cliente ID[{self.cid}] -> Chave '{key}' encontrada no conjunto de escrita do cliente. Valor é '{transaction.ws[key]}'"
            )
            return transaction.ws[key]
        print(
            f"Cliente ID[{self.cid}] -> Chave '{key}' não encontrada no conjunto de escrita do cliente."
        )

        # Lê do servidor
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.connect(self.current_server)

        request = {"type": "read", "item": key, "cid": self.cid, "tid": transaction.tid}

        print(
            f"Cliente ID[{self.cid}] -> Enviando requisição ao servidor escolhido para buscar o valor da chave '{key}'."
        )
        server_socket.send(json.dumps(request).encode())
        response = json.loads(server_socket.recv(4096).decode())
        print(f"Cliente ID[{self.cid}] -> Resposta recebida: {response}")
        server_socket.close()

        value, version = response["value"], response["version"]
        transaction.rs[key] = (value, version)
        return value

    def write(self, transaction: Transaction, key: str, value: Any):
        print(f"Cliente ID[{self.cid}] -> Operação de escrita: {key} = {value}")
        transaction.ws[key] = value

    def commit(self, transaction: Transaction) -> bool:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.connect(self.current_server)

        request = {
            "type": "commit_request",
            "cid": self.cid,
            "tid": transaction.tid,
            "rs": transaction.rs,
            "ws": transaction.ws,
        }
        print(f"Cliente ID[{self.cid}] -> Requisição de commit enviada: {request}")
        server_socket.send(json.dumps(request).encode())
        response = json.loads(server_socket.recv(4096).decode())
        server_socket.close()

        print(
            f"Cliente ID[{self.cid}] -> Resultado do commit! {response['result'] == 'commit'}"
        )
        return response["result"] == "commit"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--client-id", type=int, help="id do cliente")

    args = parser.parse_args()

    if args.client_id != None:
        client_id = args.client_id

        with open("servers_config.json") as f:
            server_config = json.load(f)

        servers = [tuple(server_config[id]) for id in server_config]

        client = Client(client_id, servers)
    else:
        print("O ID do cliente deve ser declarado! Use --help para saber mais.")

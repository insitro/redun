import os
import socket
from typing import List

from redun import task

redun_namespace = "redun.examples.aws_batch_multinode"

# Port main node uses to receive messages from workers.
PORT = 1000


@task(executor="batch", num_nodes=5, memory=1, vcpus=1)
def node_task(output_path) -> List[str]:
    """
    This task runs on each node of the multi-node cluster.

    Each instance of this task distinguishes itself using the index
    environment variable. One of the nodes is a the 'main' node and the
    others are the 'workers'. The workers can use the IP address environment
    variable to connect to the main node and begin communication. Only the
    return value of the main node will be returned to the task's caller.
    """

    # Get environment config.
    main_index = os.environ.get("AWS_BATCH_JOB_MAIN_NODE_INDEX", "-1")
    index = os.environ.get("AWS_BATCH_JOB_NODE_INDEX", "-1")
    env_num_nodes = int(os.environ.get("AWS_BATCH_JOB_NUM_NODES", "0"))
    main_address = os.environ.get("AWS_BATCH_JOB_MAIN_NODE_PRIVATE_IPV4_ADDRESS")

    message = f"Hi, I'm node {index}."
    print(message)

    if index == main_index:
        # Main node.

        # Listen for messages from workers.
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.bind((socket.gethostname(), PORT))
        serversocket.listen(env_num_nodes)

        messages = [message]

        # Receive up n - 1 messages.
        for i in range(env_num_nodes - 1):
            (clientsocket, address) = serversocket.accept()
            messages.append(clientsocket.recv(100).decode("utf-8"))
            clientsocket.close()

        serversocket.close()

        # This return value is returned to main.
        return messages
    else:
        # Worker node.

        # Send message to main node.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((main_address, PORT))
        sock.send(message.encode("utf-8"))
        sock.close()

        # This return value is discarded.
        return []


@task
def main(output_path: str, num_nodes: int = 3):
    return node_task.options(num_nodex=num_nodes)(output_path)

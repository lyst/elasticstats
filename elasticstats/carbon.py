from __future__ import absolute_import

import cPickle as pickle
import socket
import struct
import time


def walk(node, handle, transform=None, stack=None):
    stack = stack or []

    if transform:
        stack, node = transform(stack, node)

    for key, item in node.items():
        if isinstance(item, dict):
            walk(item, handle, transform, stack + [key])
        else:
            handle(stack + [key], item)


def prepare_stats(stats, prefix):
    keys = []
    now = time.time()

    def add_key(key, value):
        graphite_key = ".".join([prefix] + key)
        if isinstance(value, (int, float)):
            keys.append((graphite_key, (now, value)))

    def transform_node_keys(stack, node):
        if "nodes" in stack and "host" in node:
            stack[stack.index("nodes") + 1] = node["host"].replace(".", "-")
        return stack, node

    walk(stats, add_key, transform_node_keys)

    return keys


def send_to_graphite(host, port, data):
    payload = pickle.dumps(data)
    header = struct.pack("!L", len(payload))
    message = header + payload
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(message)
    sock.close()


def push_index_stats(graphite, stats, prefix):
    keys = prepare_stats(stats, prefix)
    send_to_graphite(graphite[0], graphite[1], keys)


def push_node_stats(graphite, stats, prefix):
    keys = prepare_stats(stats, prefix)
    send_to_graphite(graphite[0], graphite[1], keys)


def push_shard_stats(graphite, stats, prefix):
    keys = prepare_stats(stats, prefix)
    send_to_graphite(graphite[0], graphite[1], keys)

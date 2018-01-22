from __future__ import absolute_import
from __future__ import print_function

import argparse
import socket
import sys
import time

from . import carbon
from . import es


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('elasticsearch_url', help='URL for Elasticsearch')
    parser.add_argument('carbon_host', help='Hostname of the Carbon server')
    parser.add_argument('carbon_port', type=int, help='Port of the Carbon server')
    parser.add_argument('prefix', help='Graphite prefix to use')
    parser.add_argument('-p', '--poll', type=int, default=5,
                        help='How many seconds to wait before polling again. Defaults to 5')
    parser.add_argument('--master-only', action='store_true')
    args = parser.parse_args()
    carbon_server = (args.carbon_host, int(args.carbon_port))
    is_master_node = es.is_master_node(args.elasticsearch_url)

    while 1:
        if args.master_only and not is_master_node:
            print("ES is not master, skipping")
            time.sleep(60)
            continue
        start = time.time()

        try:
            index_stats = es.get_index_stats(args.elasticsearch_url)
            node_stats = es.get_all_node_stats(args.elasticsearch_url)
            shard_stats = es.get_shard_stats(args.elasticsearch_url)
        except (ValueError, socket.error) as exc:
            print(exc, file=sys.stderr)
        else:
            carbon.push_index_stats(carbon_server, index_stats, args.prefix)
            carbon.push_node_stats(carbon_server, node_stats, args.prefix)
            carbon.push_shard_stats(carbon_server, shard_stats, args.prefix)

        end = time.time()
        wait = max(1, args.poll - (end - start))
        time.sleep(wait)
    return 0


if __name__ == "__main__":
    sys.exit(main())

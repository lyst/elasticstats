from __future__ import absolute_import

import socket
import sys
import time

from . import carbon
from . import es


POLL_SECONDS = 5


def main(args):
    es_url = args[1]
    carbon_host = (args[2], int(args[3]))
    prefix = args[4]

    while 1:
        start = time.time()

        try:
            index_stats = es.get_index_stats(es_url)
            node_stats = es.get_all_node_stats(es_url)
            shard_stats = es.get_shard_stats(es_url)
        except (ValueError, socket.error) as exc:
            print >>sys.stderr, exc
            pass
        else:
            carbon.push_index_stats(carbon_host, index_stats, prefix)
            carbon.push_node_stats(carbon_host, node_stats, prefix)
            carbon.push_shard_stats(carbon_host, shard_stats, prefix)

        end = time.time()

        wait = max(1, POLL_SECONDS - (end - start))

        time.sleep(wait)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

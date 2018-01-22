from __future__ import absolute_import

import json

from future.moves.urllib.error import HTTPError
from future.moves.urllib.request import urlopen

NODES_LOCAL_PATH = "_nodes/_local"
CLUSTER_STATE_PATH = "_cluster/state/master_node"
INDEX_STATS_PATH = "_stats"
NODE_STATS_PATH = "_nodes/stats?all=true"
SHARD_STATS_PATH = "_status"


def is_master_node(es_url):
    request = urlopen("{0}/{1}".format(es_url, NODES_LOCAL_PATH))
    nodes_local = json.loads(request.read())
    request = urlopen("{0}/{1}".format(es_url, CLUSTER_STATE_PATH))
    cluster_state = json.loads(request.read())
    if list(nodes_local['nodes'].keys())[0] == cluster_state['master_node']:
        return True
    else:
        return False


def get_index_stats(es_url):
    stats_url = "{0}/{1}".format(es_url, INDEX_STATS_PATH)
    request = urlopen(stats_url)
    return json.loads(request.read())


def get_all_node_stats(es_url):
    stats_url = "{0}/{1}".format(es_url, NODE_STATS_PATH)
    request = urlopen(stats_url)
    return json.loads(request.read())


def get_shard_stats(es_url):
    stats_url = "{0}/{1}".format(es_url, SHARD_STATS_PATH)
    try:
        response = urlopen(stats_url)
    except HTTPError as exc:
        response = exc
    json_data = json.loads(response.read())
    return json_data

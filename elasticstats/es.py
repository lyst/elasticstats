from __future__ import absolute_import

import json
import urllib

NODES_LOCAL_PATH = "_nodes/_local"
CLUSTER_STATE_PATH = "_cluster/state/master_node"
INDEX_STATS_PATH = "_stats"
NODE_STATS_PATH = "_nodes/stats?all=true"
SHARD_STATS_PATH = "_status"


def is_master_node(es_url):
    request = urllib.urlopen("{0}/{1}".format(es_url, NODES_LOCAL_PATH))
    nodes_local = json.loads(request.read())
    request = urllib.urlopen("{0}/{1}".format(es_url, CLUSTER_STATE_PATH))
    cluster_state = json.loads(request.read())
    if nodes_local['nodes'].keys()[0] == cluster_state['master_node']:
        return True
    else:
        return False


def get_index_stats(es_url):
    stats_url = "{0}/{1}".format(es_url, INDEX_STATS_PATH)
    request = urllib.urlopen(stats_url)
    return json.loads(request.read())


def get_all_node_stats(es_url):
    stats_url = "{0}/{1}".format(es_url, NODE_STATS_PATH)
    request = urllib.urlopen(stats_url)
    return json.loads(request.read())


def get_shard_stats(es_url):
    stats_url = "{0}/{1}".format(es_url, SHARD_STATS_PATH)
    request = urllib.urlopen(stats_url)
    return json.loads(request.read())

from __future__ import absolute_import

import json
import time
import urllib

INDEX_STATS_PATH = "_stats"
NODE_STATS_PATH = "_nodes/stats?all=true"


def get_index_stats(es_url):
    stats_url = "{0}/{1}".format(es_url, INDEX_STATS_PATH)
    request = urllib.urlopen(stats_url)
    return json.loads(request.read())


def get_all_node_stats(es_url):
    stats_url = "{0}/{1}".format(es_url, NODE_STATS_PATH)
    request = urllib.urlopen(stats_url)
    return json.loads(request.read())

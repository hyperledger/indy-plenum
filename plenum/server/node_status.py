import json
import time

import os

import base58


STATUS_NODE_JSON_SCHEMA_VERSION = '0.0.1'


def calc_node_status(node):
    return {
        'alias': node.name,
        'bindings': {
            'client': {
                'ip': node.clientstack.ha.host,
                'port': node.clientstack.ha.port,
                'protocol': 'tcp',  # TODO hard coded for now, need more smart approach here
            },
            'node': {
                'ip': node.nodestack.ha.host,
                'port': node.nodestack.ha.port,
                'protocol': 'tcp',  # TODO hard coded for now, need more smart approach here
            }
        },
        'did': node.wallet.defaultId,
        'enabled': 'unknown',  # TODO how to implement this?
        'response-version': STATUS_NODE_JSON_SCHEMA_VERSION,
        'state': 'unknown',  # TODO how to implement this?
        'timestamp': int(time.time()),
        'verkey': base58.b58encode(node.nodestack.verKey),
        'metrics': {
            'average-per-second': {
                'read-transactions': node.total_read_request_number / (time.time() - node.created),
                'write-transactions': node.monitor.totalRequests / (time.time() - node.created),
            },
            'transaction-count': {
                'config': '???',
                'ledger': node.domainLedger.size,
                'pool': node.poolLedger.size,
            },
            'uptime': int(time.time() - node.created),
        },
        'pool': {
            'reachable': {
                'count': node.connectedNodeCount,
                'list': sorted(list(node.nodestack.conns) + [node.name]),
            },
            'unreachable': {
                'count': len(node.nodestack.remotes) - len(node.nodestack.conns),
                'list': list(set(node.nodestack.remotes.keys()) - node.nodestack.conns),
            },
            'total-count': len(node.nodestack.remotes) + 1,

        },
        'software': {
            'indy-node': '???',
            'sovrin': '???',
        }
    }


def dump_node_status(path, status):
    with open(path, 'w') as fd:
        json.dump(status, fd)


def calc_and_dump_node_status(node, base_dir):
    status = calc_node_status(node)
    file_name = '{}_node_status.json'.format(node.name.lower())
    path = os.path.join(base_dir, file_name)
    dump_node_status(path, status)

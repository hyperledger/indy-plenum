import json
import time

import os

import base58


class ValidatorNodeInfoTool:
    STATUS_NODE_JSON_SCHEMA_VERSION = '0.0.1'
    FILE_NAME_TEMPLATE = '{node_name}_info.json'

    def __init__(self, node):
        self.__node = node

    @property
    def info(self):
        return {
            'alias': self.__node.name,
            'bindings': {
                'client': {
                    'ip': self.__node.clientstack.ha.host,
                    'port': self.__node.clientstack.ha.port,
                    'protocol': 'tcp',  # TODO hard coded for now, need more smart approach here
                },
                'node': {
                    'ip': self.__node.nodestack.ha.host,
                    'port': self.__node.nodestack.ha.port,
                    'protocol': 'tcp',  # TODO hard coded for now, need more smart approach here
                }
            },
            'did': self.__node.wallet.defaultId,
            'response-version': self.STATUS_NODE_JSON_SCHEMA_VERSION,
            'timestamp': int(time.time()),
            'verkey': base58.b58encode(self.__node.nodestack.verKey),
            'metrics': {
                'average-per-second': {
                    'read-transactions': self.__node.total_read_request_number / (time.time() - self.__node.created),
                    'write-transactions': self.__node.monitor.totalRequests / (time.time() - self.__node.created),
                },
                'transaction-count': {
                    'ledger': self.__node.domainLedger.size,
                    'pool': self.__node.poolLedger.size,
                },
                'uptime': int(time.time() - self.__node.created),
            },
            'pool': {
                'reachable': {
                    'count': self.__node.connectedNodeCount,
                    'list': sorted(list(self.__node.nodestack.conns) + [self.__node.name]),
                },
                'unreachable': {
                    'count': len(self.__node.nodestack.remotes) - len(self.__node.nodestack.conns),
                    'list': list(set(self.__node.nodestack.remotes.keys()) - self.__node.nodestack.conns),
                },
                'total-count': len(self.__node.nodestack.remotes) + 1,

            }
        }

    def dump_json_file(self):
        file_name = self.FILE_NAME_TEMPLATE.format(node_name=self.__node.name.lower())
        path = os.path.join(self.__node.basedirpath, file_name)
        with open(path, 'w') as fd:
            json.dump(self.info, fd)

import json
import time

import os

import base58


def none_on_fail(func):

    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            return None
    return wrap


class ValidatorNodeInfoTool:
    JSON_SCHEMA_VERSION = '0.0.1'
    FILE_NAME_TEMPLATE = '{node_name}_info.json'

    def __init__(self, node):
        self._node = node
        self.__name = self._node.name
        self.__base_path = self._node.basedirpath

    @property
    def info(self):
        return {
            'alias': self.__alias,
            'bindings': {
                'client': {
                    'ip': self.__clientstack_ip,
                    'port': self.__clientstack_port,
                    'protocol': 'tcp',  # TODO hard coded for now, need more smart approach here
                },
                'node': {
                    'ip': self.__nodestack_ip,
                    'port': self.__nodestack_port,
                    'protocol': 'tcp',  # TODO hard coded for now, need more smart approach here
                }
            },
            'did': self.__did,
            'response-version': self.JSON_SCHEMA_VERSION,
            'timestamp': int(time.time()),
            'verkey': self.__verkey,
            'metrics': {
                'average-per-second': {
                    'read-transactions': self.__avg_read,
                    'write-transactions': self.__avg_write,
                },
                'transaction-count': {
                    'ledger': self.__domain_ledger_size,
                    'pool': self.__pool_ledger_size,
                },
                'uptime': self.__uptime,
            },
            'pool': {
                'reachable': {
                    'count': self.__reachable_count,
                    'list': self.__reachable_list,
                },
                'unreachable': {
                    'count': self.__unreachable_count,
                    'list': self.__unreachable_list,
                },
                'total-count': self.__total_count,
            }
        }

    @property
    @none_on_fail
    def __alias(self):
        return self._node.name

    @property
    @none_on_fail
    def __clientstack_ip(self):
        return self._node.clientstack.ha.host

    @property
    @none_on_fail
    def __clientstack_port(self):
        return self._node.clientstack.ha.port

    @property
    @none_on_fail
    def __nodestack_ip(self):
        return self._node.nodestack.ha.host

    @property
    @none_on_fail
    def __nodestack_port(self):
        return self._node.nodestack.ha.port

    @property
    @none_on_fail
    def __did(self):
        return self._node.wallet.defaultId

    @property
    @none_on_fail
    def __verkey(self):
        return base58.b58encode(self._node.nodestack.verKey)

    @property
    @none_on_fail
    def __avg_read(self):
        return self._node.total_read_request_number / (time.time() - self._node.created)

    @property
    @none_on_fail
    def __avg_write(self):
        return self._node.monitor.totalRequests / (time.time() - self._node.created)

    @property
    @none_on_fail
    def __domain_ledger_size(self):
        return self._node.domainLedger.size

    @property
    @none_on_fail
    def __pool_ledger_size(self):
        return self._node.poolLedger.size if self._node.poolLedger else 0

    @property
    @none_on_fail
    def __uptime(self):
        return int(time.time() - self._node.created)

    @property
    @none_on_fail
    def __reachable_count(self):
        return self._node.connectedNodeCount

    @property
    @none_on_fail
    def __reachable_list(self):
        return sorted(list(self._node.nodestack.conns) + [self._node.name])

    @property
    @none_on_fail
    def __unreachable_count(self):
        return len(self._node.nodestack.remotes) - len(self._node.nodestack.conns)

    @property
    @none_on_fail
    def __unreachable_list(self):
        return list(set(self._node.nodestack.remotes.keys()) - self._node.nodestack.conns)

    @property
    @none_on_fail
    def __total_count(self):
        return len(self._node.nodestack.remotes) + 1

    def dump_json_file(self):
        file_name = self.FILE_NAME_TEMPLATE.format(node_name=self.__name.lower())
        path = os.path.join(self.__base_path, file_name)
        with open(path, 'w') as fd:
            json.dump(self.info, fd)

import base58
import re

from plenum.common.constants import TXN_TYPE, GET_TXN, NODE
from plenum.server.validator_info_tool import ValidatorNodeInfoTool
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from stp_core.common.constants import ZMQ_NETWORK_PROTOCOL


nodeCount = 5


def test_validator_info_file_schema_is_valid(info):
    assert isinstance(info, dict)
    assert 'alias' in info

    assert 'bindings' in info
    assert 'client' in info['bindings']
    assert 'ip' not in info['bindings']['client']
    assert 'port' in info['bindings']['client']
    assert 'protocol' in info['bindings']['client']
    assert 'node' in info['bindings']
    assert 'ip' not in info['bindings']['node']
    assert 'port' in info['bindings']['node']
    assert 'protocol' in info['bindings']['node']

    assert 'did' in info
    assert 'response-version' in info
    assert 'timestamp' in info
    assert 'verkey' in info

    assert 'metrics' in info
    assert 'average-per-second' in info['metrics']
    assert 'read-transactions' in info['metrics']['average-per-second']
    assert 'write-transactions' in info['metrics']['average-per-second']
    assert 'transaction-count' in info['metrics']
    assert 'ledger' in info['metrics']['transaction-count']
    assert 'pool' in info['metrics']['transaction-count']
    assert 'uptime' in info['metrics']

    assert 'ledgers' in info
    assert 'ledger' in info['ledgers']
    assert 'pool' in info['ledgers']

    assert 'pool' in info
    assert 'reachable' in info['pool']
    assert 'count' in info['pool']['reachable']
    assert 'list' in info['pool']['reachable']
    assert 'unreachable' in info['pool']
    assert 'count' in info['pool']['unreachable']
    assert 'list' in info['pool']['unreachable']
    assert 'total-count' in info['pool']


def test_validator_info_file_alias_field_valid(info):
    assert info['alias'] == 'Alpha'


def test_validator_info_file_bindings_field_valid(info, node):
    # don't forget enable this check if ip comes back
    # assert info['bindings']['client']['ip'] == node.clientstack.ha.host
    assert 'ip' not in info['bindings']['client']
    assert info['bindings']['client']['port'] == node.clientstack.ha.port
    assert info['bindings']['client']['protocol'] == ZMQ_NETWORK_PROTOCOL

    # don't forget enable this check if ip comes back
    # assert info['bindings']['node']['ip'] == node.nodestack.ha.host
    assert 'ip' not in info['bindings']['node']
    assert info['bindings']['node']['port'] == node.nodestack.ha.port
    assert info['bindings']['node']['protocol'] == ZMQ_NETWORK_PROTOCOL


def test_validator_info_file_did_field_valid(info):
    assert info['did'] == 'JpYerf4CssDrH76z7jyQPJLnZ1vwYgvKbvcp16AB5RQ'


def test_validator_info_file_response_version_field_valid(info):
    assert (info['response-version'] ==
            ValidatorNodeInfoTool.JSON_SCHEMA_VERSION)


def test_validator_info_file_timestamp_field_valid(load_latest_info,
                                                   info):
    assert re.match('\d{10}', str(info['timestamp']))
    latest_info = load_latest_info()
    assert latest_info['timestamp'] > info['timestamp']


def test_validator_info_file_verkey_field_valid(node, info):
    assert info['verkey'] == base58.b58encode(node.nodestack.verKey)


def test_validator_info_file_metrics_avg_write_field_valid(
        info, write_txn_and_get_latest_info):
    assert info['metrics']['average-per-second']['write-transactions'] == 0
    latest_info = write_txn_and_get_latest_info()
    assert latest_info['metrics']['average-per-second']['write-transactions'] > 0  # noqa


def test_validator_info_file_metrics_avg_read_field_valid(
        info, read_txn_and_get_latest_info):
    assert info['metrics']['average-per-second']['read-transactions'] == 0
    latest_info = read_txn_and_get_latest_info(GET_TXN)
    assert latest_info['metrics']['average-per-second']['read-transactions'] > 0  # noqa


def test_validator_info_file_metrics_count_ledger_field_valid(
        poolTxnData, info):
    txns_num = sum(1 for item in poolTxnData["txns"]
                   if item.get(TXN_TYPE) != NODE)
    assert info['metrics']['transaction-count']['ledger'] == txns_num


def test_validator_info_file_metrics_count_pool_field_valid(info):
    assert info['metrics']['transaction-count']['pool'] == nodeCount


def test_validator_info_file_metrics_uptime_field_valid(
        load_latest_info, info):
    assert info['metrics']['uptime'] > 0
    latest_info = load_latest_info()
    assert latest_info['metrics']['uptime'] > info['metrics']['uptime']


def test_validator_info_file_pool_fields_valid(
        txnPoolNodesLooper, txnPoolNodeSet, info, load_latest_info):
    pool = info['pool']
    assert pool['reachable']['count'] == nodeCount
    assert (pool['reachable']['list'] ==
            sorted(list(node.name for node in txnPoolNodeSet)))
    assert pool['unreachable']['count'] == 0
    assert pool['unreachable']['list'] == []
    assert pool['total-count'] == nodeCount

    others, disconnected = txnPoolNodeSet[:-1], txnPoolNodeSet[-1]
    disconnect_node_and_ensure_disconnected(
        txnPoolNodesLooper, others, disconnected)
    latest_info = load_latest_info()

    pool = latest_info['pool']
    assert pool['reachable']['count'] == nodeCount - 1
    assert (pool['reachable']['list'] ==
            sorted(list(node.name for node in others)))
    assert pool['unreachable']['count'] == 1
    assert pool['unreachable']['list'] == [txnPoolNodeSet[-1].name]
    assert pool['total-count'] == nodeCount


def test_validator_info_file_handle_fails(info,
                                          node,
                                          load_latest_info):
    node._info_tool._node = None
    latest_info = load_latest_info()

    assert latest_info['alias'] is None
    # assert latest_info['bindings']['client']['ip'] is None
    assert 'ip' not in info['bindings']['client']
    assert latest_info['bindings']['client']['port'] is None
    # assert latest_info['bindings']['node']['ip'] is None
    assert 'ip' not in info['bindings']['node']
    assert latest_info['bindings']['node']['port'] is None
    assert latest_info['did'] is None
    assert latest_info['timestamp'] is not None
    assert latest_info['verkey'] is None
    metrics = latest_info['metrics']
    assert metrics['average-per-second']['read-transactions'] is None
    assert metrics['average-per-second']['write-transactions'] is None
    assert metrics['transaction-count']['ledger'] is None
    assert metrics['transaction-count']['pool'] is None
    assert metrics['uptime'] is None
    pool = latest_info['pool']
    assert pool['reachable']['count'] is None
    assert pool['reachable']['list'] is None
    assert pool['unreachable']['count'] is None
    assert pool['unreachable']['list'] is None
    assert pool['total-count'] is None

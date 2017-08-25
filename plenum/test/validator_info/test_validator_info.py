import json
import os
from random import randint

import base58
import pytest
import re

from plenum.common.constants import TXN_TYPE, GET_TXN, DATA, NODE
from plenum.common.request import Request
from plenum.common.util import getTimeBasedId
from plenum.server.validator_info_tool import ValidatorNodeInfoTool
from plenum.test import waits
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests, checkSufficientRepliesReceived
# noinspection PyUnresolvedReferences
from plenum.test.pool_transactions.conftest import steward1, stewardWallet, client1Connected  # noqa
from stp_core.loop.eventually import eventually


TEST_NODE_NAME = 'Alpha'
INFO_FILENAME = '{}_info.json'.format(TEST_NODE_NAME.lower())
PERIOD_SEC = 5
TXNS_COUNT = 8
nodeCount = 5


def test_validator_info_file_schema_is_valid(info):
    assert isinstance(info, dict)
    assert 'alias' in info

    assert 'bindings' in info
    assert 'client' in info['bindings']
    assert 'ip' in info['bindings']['client']
    assert 'port' in info['bindings']['client']
    assert 'protocol' in info['bindings']['client']
    assert 'node' in info['bindings']
    assert 'ip' in info['bindings']['node']
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
    assert info['bindings']['client']['ip'] == node.clientstack.ha.host
    assert info['bindings']['client']['port'] == node.clientstack.ha.port
    assert info['bindings']['client']['protocol'] == 'tcp'
    assert info['bindings']['node']['ip'] == node.nodestack.ha.host
    assert info['bindings']['node']['port'] == node.nodestack.ha.port
    assert info['bindings']['node']['protocol'] == 'tcp'


def test_validator_info_file_did_field_valid(info):
    assert info['did'] == 'JpYerf4CssDrH76z7jyQPJLnZ1vwYgvKbvcp16AB5RQ'


def test_validator_info_file_response_version_field_valid(info):
    assert info['response-version'] == ValidatorNodeInfoTool.JSON_SCHEMA_VERSION


def test_validator_info_file_timestamp_field_valid(info):
    assert re.match('\d{10}', str(info['timestamp']))


def test_validator_info_file_verkey_field_valid(node, info):
    assert info['verkey'] == base58.b58encode(node.nodestack.verKey)


def test_validator_info_file_metrics_avg_write_field_valid(info):
    assert info['metrics']['average-per-second']['write-transactions'] == 0


def test_validator_info_file_metrics_avg_read_field_valid(info):
    assert info['metrics']['average-per-second']['read-transactions'] == 0


def test_validator_info_file_metrics_count_ledger_field_valid(poolTxnData, info):
    txns_num = sum(1 for item in poolTxnData["txns"] if item.get(TXN_TYPE) != NODE)
    assert info['metrics']['transaction-count']['ledger'] == txns_num


def test_validator_info_file_metrics_count_pool_field_valid(info):
    assert info['metrics']['transaction-count']['pool'] == nodeCount


def test_validator_info_file_metrics_uptime_field_valid(info):
    assert info['metrics']['uptime'] > 0


def test_validator_info_file_pool_reachable_cnt_field_valid(info):
    assert info['pool']['reachable']['count'] == nodeCount


def test_validator_info_file_pool_reachable_list_field_valid(txnPoolNodeSet, info):
    assert info['pool']['reachable']['list'] == \
        sorted(list(node.name for node in txnPoolNodeSet))


def test_validator_info_file_pool_unreachable_cnt_field_valid(info):
    assert info['pool']['unreachable']['count'] == 0


def test_validator_info_file_pool_unreachable_list_field_valid(info):
    assert info['pool']['unreachable']['list'] == []


def test_validator_info_file_pool_total_count_field_valid(info):
    assert info['pool']['total-count'] == nodeCount


@pytest.fixture(scope='module')
def info(info_path):
    return load_info(info_path)


def load_info(path):
    with open(path) as fd:
        info = json.load(fd)
    return info


@pytest.fixture(scope='module')
def info_path(tdirWithPoolTxns, patched_dump_info_period, txnPoolNodesLooper, txnPoolNodeSet):
    path = os.path.join(tdirWithPoolTxns, INFO_FILENAME)
    assert os.path.exists(path), '{} exists'.format(path)
    return path


@pytest.fixture(scope='module')
def patched_dump_info_period(tconf):
    old_period = tconf.DUMP_VALIDATOR_INFO_PERIOD_SEC
    tconf.DUMP_VALIDATOR_INFO_PERIOD_SEC = PERIOD_SEC
    yield tconf.DUMP_VALIDATOR_INFO_PERIOD_SEC
    tconf.DUMP_VALIDATOR_INFO_PERIOD_SEC = old_period


@pytest.fixture(scope='module')
def node(txnPoolNodeSet):
    for n in txnPoolNodeSet:
        if n.name == TEST_NODE_NAME:
            return n
    assert False, 'Pool does not have "{}" node'.format(TEST_NODE_NAME)

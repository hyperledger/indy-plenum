import json
import os

import base58
import pytest
import re

from plenum.common.constants import TXN_TYPE, GET_TXN, DATA, NODE, \
    CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.common.types import f
from plenum.common.util import getTimeBasedId
from plenum.server.validator_info_tool import ValidatorNodeInfoTool
from plenum.test import waits
from plenum.test.helper import waitForSufficientRepliesForRequests, \
    sendRandomRequest, check_sufficient_replies_received
# noinspection PyUnresolvedReferences
from plenum.test.node_catchup.helper import ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.conftest import steward1, stewardWallet, client1Connected  # noqa
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_client import genTestClient
from stp_core.common.constants import ZMQ_NETWORK_PROTOCOL
from stp_core.loop.eventually import eventually


TEST_NODE_NAME = 'Alpha'
INFO_FILENAME = '{}_info.json'.format(TEST_NODE_NAME.lower())
PERIOD_SEC = 1
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
    assert info['response-version'] == ValidatorNodeInfoTool.JSON_SCHEMA_VERSION


def test_validator_info_file_timestamp_field_valid(load_latest_info,
                                                   info):
    assert re.match('\d{10}', str(info['timestamp']))
    latest_info = load_latest_info()
    assert latest_info['timestamp'] > info['timestamp']


def test_validator_info_file_verkey_field_valid(node, info):
    assert info['verkey'] == base58.b58encode(node.nodestack.verKey)


def test_validator_info_file_metrics_avg_write_field_valid(info,
                                                           write_txn_and_get_latest_info):
    assert info['metrics']['average-per-second']['write-transactions'] == 0
    latest_info = write_txn_and_get_latest_info()
    assert latest_info['metrics']['average-per-second']['write-transactions'] > 0


def test_validator_info_file_metrics_avg_read_field_valid(info,
                                                          read_txn_and_get_latest_info
                                                          ):
    assert info['metrics']['average-per-second']['read-transactions'] == 0
    latest_info = read_txn_and_get_latest_info(GET_TXN)
    assert latest_info['metrics']['average-per-second']['read-transactions'] > 0


def test_validator_info_file_metrics_count_ledger_field_valid(poolTxnData, info):
    txns_num = sum(1 for item in poolTxnData["txns"] if item.get(TXN_TYPE) != NODE)
    assert info['metrics']['transaction-count']['ledger'] == txns_num


def test_validator_info_file_metrics_count_pool_field_valid(info):
    assert info['metrics']['transaction-count']['pool'] == nodeCount


def test_validator_info_file_metrics_uptime_field_valid(load_latest_info,
                                                        info):
    assert info['metrics']['uptime'] > 0
    latest_info = load_latest_info()
    assert latest_info['metrics']['uptime'] > info['metrics']['uptime']


def test_validator_info_file_pool_fields_valid(info, txnPoolNodesLooper, txnPoolNodeSet,
                                               load_latest_info):
    assert info['pool']['reachable']['count'] == nodeCount
    assert info['pool']['reachable']['list'] == sorted(list(node.name for node in txnPoolNodeSet))
    assert info['pool']['unreachable']['count'] == 0
    assert info['pool']['unreachable']['list'] == []
    assert info['pool']['total-count'] == nodeCount

    others, disconnected = txnPoolNodeSet[:-1], txnPoolNodeSet[-1]
    disconnect_node_and_ensure_disconnected(txnPoolNodesLooper, txnPoolNodeSet, disconnected)
    latest_info = load_latest_info()

    assert latest_info['pool']['reachable']['count'] == nodeCount - 1
    assert latest_info['pool']['reachable']['list'] == sorted(list(node.name for node in others))
    assert latest_info['pool']['unreachable']['count'] == 1
    assert latest_info['pool']['unreachable']['list'] == [txnPoolNodeSet[-1].name]
    assert latest_info['pool']['total-count'] == nodeCount


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
    assert latest_info['metrics']['average-per-second']['read-transactions'] is None
    assert latest_info['metrics']['average-per-second']['write-transactions'] is None
    assert latest_info['metrics']['transaction-count']['ledger'] is None
    assert latest_info['metrics']['transaction-count']['pool'] is None
    assert latest_info['metrics']['uptime'] is None
    assert latest_info['pool']['reachable']['count'] is None
    assert latest_info['pool']['reachable']['list'] is None
    assert latest_info['pool']['unreachable']['count'] is None
    assert latest_info['pool']['unreachable']['list'] is None
    assert latest_info['pool']['total-count'] is None


@pytest.fixture(scope='module')
def info(info_path):
    return load_info(info_path)


def load_info(path):
    with open(path) as fd:
        info = json.load(fd)
    return info


@pytest.fixture(scope='module')
def info_path(patched_dump_info_period, txnPoolNodesLooper, txnPoolNodeSet, node):
    path = os.path.join(node.node_info_dir, INFO_FILENAME)
    txnPoolNodesLooper.runFor(patched_dump_info_period)
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


@pytest.fixture
def read_txn_and_get_latest_info(txnPoolNodesLooper, patched_dump_info_period,
                                 client_and_wallet, info_path):
    client, wallet = client_and_wallet

    def read_wrapped(txn_type):
        op = {
            TXN_TYPE: txn_type,
            f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
            DATA: 1
        }
        req = Request(identifier=wallet.defaultId,
                      operation=op, reqId=getTimeBasedId(),
                      protocolVersion=CURRENT_PROTOCOL_VERSION)
        client.submitReqs(req)

        timeout = waits.expectedTransactionExecutionTime(
            len(client.inBox))

        txnPoolNodesLooper.run(
            eventually(check_sufficient_replies_received,
                       client, req.identifier, req.reqId,
                       retryWait=1, timeout=timeout))
        txnPoolNodesLooper.runFor(patched_dump_info_period)
        return load_info(info_path)
    return read_wrapped


@pytest.fixture
def write_txn_and_get_latest_info(txnPoolNodesLooper,
                                  client_and_wallet,
                                  patched_dump_info_period,
                                  info_path):
    client, wallet = client_and_wallet

    def write_wrapped():
        req = sendRandomRequest(wallet, client)
        waitForSufficientRepliesForRequests(txnPoolNodesLooper, client, requests=[req])
        txnPoolNodesLooper.runFor(patched_dump_info_period)
        return load_info(info_path)
    return write_wrapped


@pytest.fixture(scope="function")
def load_latest_info(txnPoolNodesLooper, patched_dump_info_period, info_path):
    def wrapped():
        txnPoolNodesLooper.runFor(patched_dump_info_period + 1)
        return load_info(info_path)
    return wrapped


@pytest.fixture
def client_and_wallet(txnPoolNodesLooper, tdirWithClientPoolTxns, txnPoolNodeSet):
    client, wallet = genTestClient(tmpdir=tdirWithClientPoolTxns, nodes=txnPoolNodeSet,
                                   name='reader', usePoolLedger=True)
    txnPoolNodesLooper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(txnPoolNodesLooper, client,
                                                  *txnPoolNodeSet)
    return client, wallet

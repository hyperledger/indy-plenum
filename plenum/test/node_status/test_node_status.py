import json
import os
from random import randint

import base58
import pytest
import re

from plenum.common.constants import TXN_TYPE, GET_TXN, DATA
from plenum.common.request import Request
from plenum.common.util import getTimeBasedId
from plenum.test import waits
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests, checkSufficientRepliesReceived
from stp_core.loop.eventually import eventually

STATUS_FILENAME = 'alpha_node_status.json'
PERIOD_SEC = 5
TXNS_COUNT = 10


def test_node_status_file_schema_is_valid(status):
    assert isinstance(status, dict)
    assert 'alias' in status

    assert 'bindings' in status
    assert 'client' in status['bindings']
    assert 'ip' in status['bindings']['client']
    assert 'port' in status['bindings']['client']
    assert 'protocol' in status['bindings']['client']
    assert 'node' in status['bindings']
    assert 'ip' in status['bindings']['node']
    assert 'port' in status['bindings']['node']
    assert 'protocol' in status['bindings']['node']

    assert 'did' in status
    assert 'enabled' in status
    assert 'response-version' in status
    assert 'state' in status
    assert 'timestamp' in status
    assert 'verkey' in status

    assert 'metrics' in status
    assert 'average-per-second' in status['metrics']
    assert 'read-transactions' in status['metrics']['average-per-second']
    assert 'write-transactions' in status['metrics']['average-per-second']
    assert 'transaction-count' in status['metrics']
    assert 'config' in status['metrics']['transaction-count']
    assert 'ledger' in status['metrics']['transaction-count']
    assert 'pool' in status['metrics']['transaction-count']
    assert 'uptime' in status['metrics']

    assert 'pool' in status
    assert 'reachable' in status['pool']
    assert 'count' in status['pool']['reachable']
    assert 'list' in status['pool']['reachable']
    assert 'unreachable' in status['pool']
    assert 'count' in status['pool']['unreachable']
    assert 'list' in status['pool']['unreachable']
    assert 'total-count' in status['pool']

    assert 'software' in status
    assert 'indy-node' in status['software']
    assert 'sovrin' in status['software']


def test_node_status_file_alias_field_valid(status):
    assert status['alias'] == 'Alpha'


def test_node_status_file_bindings_field_valid(nodeSet, status):
    alpha = nodeSet.Alpha
    assert status['bindings']['client']['ip'] == alpha.clientstack.ha.host
    assert status['bindings']['client']['port'] == alpha.clientstack.ha.port
    assert status['bindings']['client']['protocol'] == 'tcp'
    assert status['bindings']['node']['ip'] == alpha.nodestack.ha.host
    assert status['bindings']['node']['port'] == alpha.nodestack.ha.port
    assert status['bindings']['node']['protocol'] == 'tcp'


def test_node_status_file_did_field_valid(status):
    assert status['did'] == 'not implemented'


def test_node_status_file_enabled_field_valid(status):
    assert status['enabled'] == 'unknown'


def test_node_status_file_response_version_field_valid(status):
    assert status['response-version'] == '0.0.1'


def test_node_status_file_state_field_valid(status):
    assert status['state'] == 'unknown'


def test_node_status_file_timestamp_field_valid(status):
    assert re.match('\d{10}', str(status['timestamp']))


def test_node_status_file_verkey_field_valid(nodeSet, status):
    alpha = nodeSet.Alpha
    assert status['verkey'] == base58.b58encode(alpha.nodestack.verKey)


def test_node_status_file_metrics_avg_write_field_valid(looper, status_path, status):
    assert status['metrics']['average-per-second']['write-transactions'] > 0


def test_node_status_file_metrics_avg_read_field_valid(looper, status_path, status):
    assert status['metrics']['average-per-second']['read-transactions'] > 0


def test_node_status_file_metrics_count_config_field_valid(looper, status_path, status):
    assert status['metrics']['transaction-count']['config'] == 'unknown'


def test_node_status_file_metrics_count_ledger_field_valid(looper, status_path, status):
    assert status['metrics']['transaction-count']['ledger'] == TXNS_COUNT


def test_node_status_file_metrics_count_pool_field_valid(looper, status_path, status):
    assert status['metrics']['transaction-count']['pool'] == 0


def test_node_status_file_metrics_uptime_field_valid(looper, status_path, status):
    assert status['metrics']['uptime'] > 0


def test_node_status_file_pool_reachable_cnt_field_valid(looper, status_path, status):
    assert status['pool']['reachable']['count'] == 4


def test_node_status_file_pool_reachable_list_field_valid(looper, nodeSet, status_path, status):
    assert status['pool']['reachable']['list'] == sorted(list(node.name for node in nodeSet))


def test_node_status_file_pool_unreachable_cnt_field_valid(looper, status_path, status):
    assert status['pool']['unreachable']['count'] == 0


def test_node_status_file_pool_unreachable_list_field_valid(looper, nodeSet, status_path, status):
    assert status['pool']['unreachable']['list'] == []


def test_node_status_file_pool_total_count_field_valid(looper, nodeSet, status_path, status):
    assert status['pool']['total-count'] == 4


def test_node_status_file_software_indy_node_field_valid(looper, nodeSet, status_path, status):
    assert status['software']['indy-node'] == 'unknown'


def test_node_status_file_software_sovrin_field_valid(looper, nodeSet, status_path, status):
    assert status['software']['sovrin'] == 'unknown'


@pytest.fixture(scope='module')
def patched_dump_status_period(tconf):
    old_period = tconf.DUMP_NODE_STATUS_PERIOD_SEC
    tconf.DUMP_NODE_STATUS_PERIOD_SEC = PERIOD_SEC
    yield tconf.DUMP_NODE_STATUS_PERIOD_SEC
    tconf.DUMP_NODE_STATUS_PERIOD_SEC = old_period


@pytest.fixture(scope='module')
def writes(looper, nodeSet, wallet1, client1):
    reqs = sendRandomRequests(wallet1, client1, TXNS_COUNT)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)
    return reqs


@pytest.fixture(scope='module')
def reads(looper, nodeSet, client1, wallet1):
    for i in range(1, 10):
        op = {
            TXN_TYPE: GET_TXN,
            DATA: i
        }
        req = Request(identifier=wallet1.defaultId,
                      operation=op, reqId=getTimeBasedId())
        client1.submitReqs(req)

        timeout = waits.expectedTransactionExecutionTime(len(nodeSet))
        looper.run(
            eventually(checkSufficientRepliesReceived, client1.inBox,
                       req.reqId, 0, retryWait=1, timeout=timeout))


@pytest.fixture(scope='module')
def status_path(tdir, patched_dump_status_period, looper, nodeSet, up, writes, reads):
    looper.runFor(PERIOD_SEC)
    path = os.path.join(tdir, STATUS_FILENAME)
    assert os.path.exists(path), '{} exists'.format(path)
    return path


def load_status(path):
    with open(path) as fd:
        status = json.load(fd)
    return status


@pytest.fixture(scope='module')
def status(status_path):
    return load_status(status_path)

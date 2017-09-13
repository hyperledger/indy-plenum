import pytest

import json
import os

from plenum.common.constants import TXN_TYPE, DATA
from plenum.common.request import Request
from plenum.common.util import getTimeBasedId
from plenum.test import waits
from plenum.test.conftest import getValueFromModule
from plenum.test.helper import waitForSufficientRepliesForRequests, checkSufficientRepliesReceived, \
    sendRandomRequest
# noinspection PyUnresolvedReferences
from plenum.test.node_catchup.helper import ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.conftest import steward1, stewardWallet, \
        stewardAndWallet1 # noqa
from plenum.test.test_client import genTestClient
from stp_core.loop.eventually import eventually



def load_info(path):
    with open(path) as fd:
        info = json.load(fd)
    return info


@pytest.fixture(scope="module")
def test_node_name(request):
    return getValueFromModule(request, "TEST_NODE_NAME", 'Alpha')


@pytest.fixture(scope='module')
def info_filename(test_node_name):
    return '{}_info.json'.format(test_node_name.lower())


@pytest.fixture(scope='module')
def patched_dump_info_period(request, tconf):
    old_period = tconf.DUMP_VALIDATOR_INFO_PERIOD_SEC
    tconf.DUMP_VALIDATOR_INFO_PERIOD_SEC = \
            getValueFromModule(request, "PERIOD_SEC", 1)
    yield tconf.DUMP_VALIDATOR_INFO_PERIOD_SEC
    tconf.DUMP_VALIDATOR_INFO_PERIOD_SEC = old_period


@pytest.fixture(scope='module')
def info_path(
        info_filename,
        tdirWithPoolTxns, patched_dump_info_period,
        txnPoolNodesLooper, txnPoolNodeSet):
    path = os.path.join(tdirWithPoolTxns, info_filename)
    txnPoolNodesLooper.runFor(patched_dump_info_period)
    return path


@pytest.fixture(scope='module')
def latest_info_wait_time(patched_dump_info_period):
    return patched_dump_info_period + 1


@pytest.fixture(scope="function")
def load_latest_info(txnPoolNodesLooper, latest_info_wait_time, info_path):
    def wrapped():
        txnPoolNodesLooper.runFor(latest_info_wait_time)
        return load_info(info_path)
    return wrapped


@pytest.fixture(scope='module')
def info(info_path):
    return load_info(info_path)



@pytest.fixture(scope='module')
def node(txnPoolNodeSet, test_node_name):
    for n in txnPoolNodeSet:
        if n.name == test_node_name:
            return n
    assert False, 'Pool does not have "{}" node'.format(test_node_name)


@pytest.fixture
def read_txn_and_get_latest_info(txnPoolNodesLooper, patched_dump_info_period,
                                 client_and_wallet, info_path):
    client, wallet = client_and_wallet

    def read_wrapped(txn_type):
        op = {
            TXN_TYPE: txn_type,
            DATA: 1
        }
        req = Request(identifier=wallet.defaultId,
                      operation=op, reqId=getTimeBasedId())
        client.submitReqs(req)

        timeout = waits.expectedTransactionExecutionTime(
            len(client.inBox))
        txnPoolNodesLooper.run(
            eventually(checkSufficientRepliesReceived, client.inBox,
                       req.reqId, 1,
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


@pytest.fixture
def client_and_wallet(txnPoolNodesLooper, tdirWithPoolTxns, txnPoolNodeSet):
    client, wallet = genTestClient(tmpdir=tdirWithPoolTxns, nodes=txnPoolNodeSet,
                                   name='reader', usePoolLedger=True)
    txnPoolNodesLooper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(txnPoolNodesLooper, client,
                                                  *txnPoolNodeSet)
    return client, wallet


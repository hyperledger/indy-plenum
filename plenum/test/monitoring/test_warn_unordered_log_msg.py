import pytest

from plenum.test.malicious_behaviors_node import delaysCommitProcessing
from plenum.test.test_node import getNonPrimaryReplicas
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_requests

nodeCount = 4
logger = getlogger()
whitelist = ['Consensus for ReqId:']

UNORDERED_CHECK_FREQ = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    oldUnorderedCheckFreq = tconf.UnorderedCheckFreq
    tconf.UnorderedCheckFreq = UNORDERED_CHECK_FREQ
    yield tconf
    tconf.UnorderedCheckFreq = oldUnorderedCheckFreq


@pytest.fixture(scope="module")
def txnPoolNodeSet(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        install_unordered_requests_watcher(node.monitor)
    yield txnPoolNodeSet


# noinspection PyIncorrectDocstring
def test_working_has_no_warn_log_msg(looper, txnPoolNodeSet,
                                     sdk_pool_handle, sdk_wallet_client):
    clear_unordered_requests(*txnPoolNodeSet)

    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 5)
    looper.runFor(1.2 * UNORDERED_CHECK_FREQ)

    assert all(len(node.monitor.unordered_requests) == 0 for node in txnPoolNodeSet)


# noinspection PyIncorrectDocstring
def test_slow_node_has_warn_unordered_log_msg(looper,
                                              txnPoolNodeSet,
                                              sdk_pool_handle,
                                              sdk_wallet_client):
    clear_unordered_requests(*txnPoolNodeSet)

    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[0].node
    delaysCommitProcessing(slow_node, delay=3 * UNORDERED_CHECK_FREQ)

    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 5)
    looper.runFor(2 * UNORDERED_CHECK_FREQ)

    assert all(len(node.monitor.unordered_requests) == 0 for node in txnPoolNodeSet if node.name != slow_node.name)
    assert len(slow_node.monitor.unordered_requests) != 0

    # Check that after being ordered request is no longer logged
    looper.runFor(2 * UNORDERED_CHECK_FREQ)
    clear_unordered_requests(*txnPoolNodeSet)
    looper.runFor(2 * UNORDERED_CHECK_FREQ)
    assert all(len(node.monitor.unordered_requests) == 0 for node in txnPoolNodeSet)


def install_unordered_requests_watcher(monitor):
    def handler(requests):
        monitor.unordered_requests.extend(requests)

    monitor.unordered_requests = []
    monitor.unordered_requests_handlers.append(handler)


def clear_unordered_requests(*nodes):
    for node in nodes:
        node.monitor.unordered_requests = []

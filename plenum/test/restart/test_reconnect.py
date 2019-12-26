import random
import pytest

from plenum.test.helper import view_change_timeout
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node

nodeCount = 8

SHORT_DISCONNECT = 2
LONG_DISCONNECT = 17
RAND_INDEXES = [random.randrange(nodeCount) for _ in range(nodeCount * 2)]


@pytest.fixture(scope="module")
def tconf(tconf):
    with view_change_timeout(tconf, 5):
        yield tconf


@pytest.fixture(params=[SHORT_DISCONNECT, LONG_DISCONNECT])
def timeout(request):
    return request.param


def restart_node(node, looper, txnPoolNodeSet, tdir, tconf, allPluginsPath, timeout):
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node)
    txnPoolNodeSet.remove(node)
    looper.removeProdable(name=node.name)

    looper.runFor(timeout)

    node = start_stopped_node(node, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(node)
    return node


def test_reconnect(looper, txnPoolNodeSet, tdir, tconf, allPluginsPath, timeout):
    index = random.choice(RAND_INDEXES)
    node_to_restart = txnPoolNodeSet[index]

    restart_node(node_to_restart, looper, txnPoolNodeSet, tdir, tconf, allPluginsPath, timeout=timeout)

    assert len(txnPoolNodeSet) == nodeCount
    looper.run(checkNodesConnected(txnPoolNodeSet))


def test_reconnect_after_successive_disconnects(looper, txnPoolNodeSet, tdir, tconf, allPluginsPath, timeout):
    for _ in range(2):
        index = RAND_INDEXES[-1]
        node_to_restart = txnPoolNodeSet[index]

        restart_node(node_to_restart, looper, txnPoolNodeSet, tdir, tconf, allPluginsPath, timeout=timeout)

        assert len(txnPoolNodeSet) == nodeCount
        looper.run(checkNodesConnected(txnPoolNodeSet))


def test_reconnect_after_multiple_random_node_disconnects(looper, txnPoolNodeSet, tdir, tconf, allPluginsPath, timeout):
    for i in range(2):
        index = random.choice(RAND_INDEXES)
        node_to_restart = txnPoolNodeSet[index]

        restart_node(node_to_restart, looper, txnPoolNodeSet, tdir, tconf, allPluginsPath, timeout=timeout)

        assert len(txnPoolNodeSet) == nodeCount
        looper.run(checkNodesConnected(txnPoolNodeSet))

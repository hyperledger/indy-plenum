import operator

import pytest

from plenum.test.helper import sdk_send_random_and_check
from plenum.test.primary_selection.helper import \
    check_rank_consistent_across_each_node
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually
from plenum.common.util import getNoInstances
from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.test_node import checkProtocolInstanceSetup, getPrimaryReplica, ensureElectionsDone

nodeCount = 7


@pytest.fixture()
def primaryReplicas(txnPoolNodeSet):
    instanceCount = getNoInstances(nodeCount)
    return [getPrimaryReplica(txnPoolNodeSet, i) for i in range(instanceCount)]


# noinspection PyIncorrectDocstring
def testPrimarySelectionAfterPoolReady(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward):
    """
    Once the pool is ready(node has connected to at least 3 other nodes),
    appropriate primary replicas should be selected.
    """

    def checkPrimaryPlacement():
        # Node names sorted by rank
        sortedNodes = sorted(txnPoolNodeSet,
                             key=operator.attrgetter("rank"))

        for idx, node in enumerate(sortedNodes):
            # For instance 0, the primary replica should be on the node with
            # rank 0
            if idx == 0:
                Replica.generateName(sortedNodes[idx].name, 0)
                assert node.replicas[0].isPrimary
                assert not node.replicas[1].isPrimary
                assert not node.replicas[2].isPrimary

            # For instance 1, the primary replica should be on the node with
            # rank 1
            if idx == 1:
                Replica.generateName(sortedNodes[idx].name, 1)
                assert not node.replicas[0].isPrimary
                assert node.replicas[1].isPrimary
                assert not node.replicas[2].isPrimary

            # For instance 2, the primary replica should be on the node with
            # rank 2
            if idx == 2:
                Replica.generateName(sortedNodes[idx].name, 2)
                assert not node.replicas[0].isPrimary
                assert not node.replicas[1].isPrimary
                assert node.replicas[2].isPrimary

    check_rank_consistent_across_each_node(txnPoolNodeSet)
    # Check if the primary is on the correct node
    timeout = waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(checkPrimaryPlacement, retryWait=1, timeout=timeout))
    # Check if every protocol instance has one and only one primary and any node
    #  has no more than one primary
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 5)


@pytest.fixture(scope='module')
def catchup_complete_count(txnPoolNodeSet):
    return {n.name: n.spylog.count(n.allLedgersCaughtUp) for n in txnPoolNodeSet}


@pytest.fixture(scope='module')  # noqa
def view_change_done(looper, txnPoolNodeSet):
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)


# noinspection PyIncorrectDocstring


def testPrimarySelectionAfterViewChange(  # noqa
        looper,
        txnPoolNodeSet,
        primaryReplicas,
        catchup_complete_count):
    """
    Test that primary replica of a protocol instance shifts to a new node after
    a view change.
    """
    # TODO: This test can fail due to view change.

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    for n in txnPoolNodeSet:
        assert n.spylog.count(
            n.allLedgersCaughtUp) > catchup_complete_count[n.name]

    # Primary replicas before view change
    prBeforeVC = primaryReplicas

    # Primary replicas after view change
    instanceCount = getNoInstances(nodeCount)
    prAfterVC = [getPrimaryReplica(txnPoolNodeSet, i) for i in range(instanceCount)]

    # Primary replicas have moved to the next node
    for br, ar in zip(prBeforeVC, prAfterVC):
        assert ar.node.rank - br.node.rank == 1

    check_rank_consistent_across_each_node(txnPoolNodeSet)
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)

import pytest

from plenum.test import waits
from plenum.test.delayers import cqDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.node_request_helper import chk_commits_prepares_recvd
from plenum.test.test_node import ensureElectionsDone
from stp_core.common.log import getlogger

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5
catchup_delay = 20


@pytest.fixture(scope="module")
def nodes_slow_to_process_catchup_reqs(txnPoolNodeSet):
    """
    This will make the new node slow to complete the catchup and hence will
    not send any 3PC messages till catchup is complete
    """
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cqDelay(catchup_delay))


def testNodeDoesNotParticipateUntilCaughtUp(txnPoolNodeSet,
                                            nodes_slow_to_process_catchup_reqs,
                                            sdk_node_created_after_some_txns):
    """
    A new node that joins after some transactions should stash new transactions
    until it has caught up
    :return:
    """

    looper, new_node, sdk_pool_handle, new_steward_wallet_handle = \
        sdk_node_created_after_some_txns
    txnPoolNodeSet.append(new_node)
    old_nodes = txnPoolNodeSet[:-1]
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              new_steward_wallet_handle, 5)
    chk_commits_prepares_recvd(0, old_nodes, new_node)

    for node in old_nodes:
        node.reset_delays_and_process_delayeds()

    timeout = waits.expectedPoolCatchupTime(len(txnPoolNodeSet)) + \
              catchup_delay + \
              waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=timeout)
    waitNodeDataEquality(looper, new_node, *old_nodes)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              new_steward_wallet_handle, 2)

    # Commits and Prepares are received by all old nodes
    with pytest.raises(AssertionError):
        # Since nodes discard 3PC messages for already ordered requests.
        chk_commits_prepares_recvd(0, old_nodes, new_node)
    waitNodeDataEquality(looper, new_node, *old_nodes)

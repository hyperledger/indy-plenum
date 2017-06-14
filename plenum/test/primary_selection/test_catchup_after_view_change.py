import pytest

from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, \
    waitNodeDataUnequality
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, nodeThetaAdded, \
    stewardAndWallet1, steward1, stewardWallet

from plenum.test.delayers import cDelay, delay_3pc_messages
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas, \
    checkProtocolInstanceSetup, TestReplica
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually


TestRunningTimeLimitSec = 150


@pytest.fixture(scope='module', params=['next-non-primary', 'next-primary'])
def slow_node(request, txnPoolNodeSet):
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    if request.param == 'next-non-primary':
        return nprs[-1].node
    if request.param == 'next-primary':
        return nprs[0].node


def test_slow_nodes_catchup_before_selecting_primary_in_new_view(looper,
                                                                 txnPoolNodeSet,
                                                                 steward1,
                                                                 stewardWallet,
                                                                 tconf,
                                                                 slow_node):
    """
    Delay 3PC to 1 node and then cause view change so by the time the view
    change happens(each node gets >2f+1 `INSTANCE_CHANGE`s), the slow node is
    behind other nodes. The should initiate catchup to come to the same state
    as other nodes. Not sending client requests while catching up, that will
    be tested in another test.
    """
    # nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    # slow_node = nprs[-1].node

    fast_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    delay = tconf.PerfCheckFreq
    # Bad network introduced
    delay_3pc_messages([slow_node], 0, delay=delay)
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 40)
    waitNodeDataUnequality(looper, slow_node, *fast_nodes)

    catchup_reply_counts = {n.name: n.ledgerManager.spylog.count(
        n.ledgerManager.processCatchupRep) for n in txnPoolNodeSet}
    catchup_done_counts = {n.name: n.spylog.count(n.allLedgersCaughtUp)
                           for n in txnPoolNodeSet}

    # No reverts have been called by the slow node
    rv = getAllReturnVals(slow_node.replicas[0],
                          TestReplica.revert_unordered_batches)
    assert not rv or max(rv) == 0

    def slow_node_processed_some():
        assert slow_node.replicas[0].batches

    looper.run(eventually(slow_node_processed_some, retryWait=1))
    ensure_view_change(looper, txnPoolNodeSet)
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    # `slow_node` does catchup, `fast_nodes` don't
    for n in txnPoolNodeSet:
        assert n.spylog.count(n.allLedgersCaughtUp) > catchup_done_counts[
            n.name]
        if n == slow_node:
            assert n.ledgerManager.spylog.count(
                n.ledgerManager.processCatchupRep) > catchup_reply_counts[n.name]
        else:
            assert n.ledgerManager.spylog.count(
                n.ledgerManager.processCatchupRep) == catchup_reply_counts[n.name]

    # Greater than 0 batches were reverted by the slow node
    assert max(getAllReturnVals(slow_node.replicas[0],
                                TestReplica.revert_unordered_batches)) > 0

    # Bad network repaired
    slow_node.reset_delays_and_process_delayeds()

    # Make sure pool is functional
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 5)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)


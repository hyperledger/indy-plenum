import pytest

from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, \
    waitNodeDataInequality
from plenum.test.delayers import cr_delay, ppDelay, pDelay, \
    cDelay
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas, \
    checkProtocolInstanceSetup, TestReplica
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually
from plenum.test.batching_3pc.conftest import tconf

Max3PCBatchSize = 4


@pytest.fixture(scope='module', params=['next-non-primary', 'next-primary'])
def slow_node(request, txnPoolNodeSet):
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    if request.param == 'next-non-primary':
        return nprs[-1].node
    if request.param == 'next-primary':
        return nprs[0].node


@pytest.mark.skip(reasone="It's an intermittent test, INDY-722")
def test_slow_nodes_catchup_before_selecting_primary_in_new_view(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, tconf, slow_node):
    """
    Delay 3PC to 1 node and then cause view change so by the time the view
    change happens(each node gets >n-f `INSTANCE_CHANGE`s), the slow node is
    behind other nodes. The should initiate catchup to come to the same state
    as other nodes.
    """

    fast_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    delay = tconf.PerfCheckFreq

    # Bad network introduced
    slow_node.nodeIbStasher.delay(ppDelay(delay, 0))
    slow_node.nodeIbStasher.delay(pDelay(2 * delay, 0))
    slow_node.nodeIbStasher.delay(cDelay(3 * delay, 0))
    for i in range(2):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_steward, 20)
        waitNodeDataInequality(looper, slow_node, *fast_nodes)

    catchup_reply_counts = {n.name: n.ledgerManager.spylog.count(
        n.ledgerManager.processCatchupRep) for n in txnPoolNodeSet}
    catchup_done_counts = {n.name: n.spylog.count(n.allLedgersCaughtUp)
                           for n in txnPoolNodeSet}

    def slow_node_processed_some():
        assert slow_node.master_replica.batches

    # The slow node has received some PRE-PREPAREs
    looper.run(
        eventually(
            slow_node_processed_some,
            retryWait=1,
            timeout=delay))

    # No reverts have been called by the slow node
    rv = getAllReturnVals(slow_node.replicas[0],
                          TestReplica.revert_unordered_batches)
    assert not rv or max(rv) == 0

    # Delay reception of catchup replies so ViewChangeDone can be received
    # before catchup completes
    delay_catchup_reply = 2
    slow_node.nodeIbStasher.delay(cr_delay(delay_catchup_reply))

    ensure_view_change(looper, txnPoolNodeSet)
    # `slow_node` will not have elections done but others will.
    checkProtocolInstanceSetup(looper, fast_nodes,
                               instances=list(slow_node.replicas.keys()),
                               retryWait=1)
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
    assert max(
        getAllReturnVals(
            slow_node.master_replica,
            slow_node.master_replica.revert_unordered_batches)) > 0

    # Bad network repaired
    slow_node.reset_delays_and_process_delayeds()

    # Make sure pool is functional
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward, 5)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

import types

import pytest

from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataUnequality, \
    ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas, \
    checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change


def test_caught_up_for_current_view_check(looper,
                                          txnPoolNodeSet,
                                          client1,
                                          wallet1,
                                          client1Connected):
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    bad_node = nprs[-1].node
    other_nodes = [n for n in txnPoolNodeSet if n != bad_node]
    orig_method = bad_node.master_replica.dispatchThreePhaseMsg

    # Bad node does not process any 3 phase messages, equivalent to messages
    # being lost
    def bad_method(self, m, s):
        pass

    bad_node.master_replica.dispatchThreePhaseMsg = types.MethodType(
        bad_method, bad_node.master_replica)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 10)
    waitNodeDataUnequality(looper, bad_node, *other_nodes)

    def caught_up_for_current_view_count():
        return sum([1 for rv in getAllReturnVals(bad_node,
                                                 bad_node.caught_up_for_current_view)
                    if rv == True])

    old_count = caught_up_for_current_view_count()
    ensure_view_change(looper, txnPoolNodeSet)
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    # The bad_node caught up due to receiving sufficient ViewChangeDone messages
    assert caught_up_for_current_view_count() > old_count

    bad_node.master_replica.dispatchThreePhaseMsg = types.MethodType(
        orig_method, bad_node.master_replica)

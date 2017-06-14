import types

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.types import f
from plenum.common.util import updateNamedTuple
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataUnequality, \
    ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas, \
    checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.batching_3pc.conftest import tconf


Max3PCBatchSize = 2


def test_caught_up_for_current_view_check(looper,
                                          txnPoolNodeSet,
                                          client1,
                                          wallet1,
                                          client1Connected):
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        3*Max3PCBatchSize)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

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

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        6*Max3PCBatchSize)
    waitNodeDataUnequality(looper, bad_node, *other_nodes)

    # Patch all nodes to return ConsistencyProof of a smaller ledger to the
    # bad node but only once, so that the bad_node needs to do catchup again.

    nodes_to_send_proof_of_small_ledger = {n.name for n in other_nodes}
    orig_methods = {}
    for node in other_nodes:
        orig_methods[node.name] = node.ledgerManager._buildConsistencyProof

        def patched_method(self, ledgerId, seqNoStart, seqNoEnd):
            if self.owner.name in nodes_to_send_proof_of_small_ledger:
                import inspect
                curframe = inspect.currentframe()
                calframe = inspect.getouterframes(curframe, 2)
                # For domain ledger, send a proof for a small ledger to the bad node
                if calframe[1][3] == node.ledgerManager.getConsistencyProof.__name__ \
                        and calframe[2].frame.f_locals['frm'] == bad_node.name \
                        and ledgerId == DOMAIN_LEDGER_ID:
                    # Pop so this node name, so proof for smaller ledger is not
                    # served again
                    nodes_to_send_proof_of_small_ledger.remove(self.owner.name)
                    return orig_methods[node.name](ledgerId, seqNoStart,
                                                   seqNoEnd-Max3PCBatchSize)
            return orig_methods[node.name](ledgerId, seqNoStart, seqNoEnd)

        node.ledgerManager._buildConsistencyProof = types.MethodType(
            patched_method, node.ledgerManager)

    def is_catchup_needed_count():
        return sum([1 for rv in getAllReturnVals(bad_node,
                                                 bad_node.is_catchup_needed) if rv == True])

    def caught_up_for_current_view_count():
        return sum([1 for rv in getAllReturnVals(bad_node,
                                                 bad_node.caught_up_for_current_view)
                    if rv is True])

    old_count_1 = is_catchup_needed_count()
    old_count_2 = caught_up_for_current_view_count()
    ensure_view_change(looper, txnPoolNodeSet)
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    assert is_catchup_needed_count() > old_count_1
    # The bad_node caught up due to receiving sufficient ViewChangeDone messages
    assert caught_up_for_current_view_count() > old_count_2

    bad_node.master_replica.dispatchThreePhaseMsg = types.MethodType(
        orig_method, bad_node.master_replica)

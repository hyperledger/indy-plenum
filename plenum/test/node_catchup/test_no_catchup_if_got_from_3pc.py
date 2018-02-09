from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Commit, ConsistencyProof
from plenum.test.delayers import cpDelay, cDelay
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper

from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, \
    waitNodeDataInequality, waitNodeDataEquality
from plenum.test.primary_selection.test_primary_selection_pool_txn import \
    ensure_pool_functional
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.view_change.helper import ensure_view_change


def test_no_catchup_if_got_from_3pc(looper, txnPoolNodeSet, wallet1, client1,
                                    client1Connected):
    """
    A node is slow to receive COMMIT messages so after a view change it
    starts catchup. But before it can start requesting txns, the COMMITs messages
    are received and are ordered. The node should not request any transactions.
    :return:
    """
    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1, 2 * 3, 3)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]

    delay_cm = 30
    delat_cp = 100
    slow_node.nodeIbStasher.delay(cDelay(delay_cm))
    # The slow node receives consistency proofs after some delay, this delay
    # gives the opportunity to deliver all 3PC messages
    slow_node.nodeIbStasher.delay(cpDelay(delat_cp))

    # Count of `getCatchupReqs` which is called to construct the `CatchupReq`
    # to be sent
    def domain_cr_count(): return sum(1 for entry in
                                      slow_node.ledgerManager.spylog.getAll(
                                          slow_node.ledgerManager.getCatchupReqs) if
                                      entry.params['consProof'].ledgerId == DOMAIN_LEDGER_ID)

    old_count = domain_cr_count()
    sent_batches = 10
    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1,
                                           2 * sent_batches, sent_batches)
    ensure_view_change(looper, nodes=txnPoolNodeSet)

    # After view change, the `slow_node` is behind
    waitNodeDataInequality(looper, slow_node, *other_nodes)

    # Unstash only COMMIT messages
    slow_node.nodeIbStasher.reset_delays_and_process_delayeds(Commit.typename)

    looper.runFor(2)

    slow_node.nodeIbStasher.reset_delays_and_process_delayeds(
        ConsistencyProof.typename)

    waitNodeDataEquality(looper, slow_node, *other_nodes)

    # No `CatchupReq`s constructed, hence no `CatchupReq`s could have
    # been sent
    assert domain_cr_count() == old_count
    # Some stashed ordered requests have been processed
    rv = getAllReturnVals(slow_node, slow_node.processStashedOrderedReqs)
    assert sent_batches in rv

    ensure_pool_functional(looper, txnPoolNodeSet, wallet1, client1)

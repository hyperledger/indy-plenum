import types

from plenum.test.delayers import nom_delay
from plenum.test.pool_transactions.conftest import clientAndWallet1, client1, \
    wallet1, client1Connected, looper
from plenum.test.helper import checkViewNoForNodes, countDiscarded, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.malicious_behaviors_node import slow_primary
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica
from plenum.test.view_change.helper import do_vc
from stp_core.common.log import getlogger

logger = getlogger()

def test_master_primary_different_from_previous_view_malicious(txnPoolNodeSet,
                                                 looper, client1,
                                                 wallet1, client1Connected):
    """
    After a view change, primary must be different from previous primary for
    master instance, it does not matter for other instance. Break it into
    2 tests, one where the primary is malign and votes for itself but is still
    not made primary in the next view.
    """
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    pr = slow_primary(txnPoolNodeSet, 0, delay=10)
    old_pr_node = pr.node

    def _get_undecided_inst_id(self):
        undecideds = [i for i, r in enumerate(self.replicas)
                      if r.isPrimary is None]
        # Try to nominate for the master instance
        return undecideds, 0

    # Patching old primary's elector's method to nominate itself
    # again for the the new view
    old_pr_node.elector._get_undecided_inst_id = types.MethodType(
        _get_undecided_inst_id, old_pr_node.elector)
    # Delay nominates so it only waits for others
    old_pr_node.nodeIbStasher.delay(nom_delay(1, 0))

    # View change happens
    do_vc(looper, txnPoolNodeSet, client1, wallet1, old_view_no)
    logger.debug("VIEW HAS BEEN CHANGED!")

    # Elections done
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # New primary is not same as old primary
    assert getPrimaryReplica(txnPoolNodeSet, 0).node.name != old_pr_node.name

    # All other nodes discarded the nomination by the old primary
    for node in txnPoolNodeSet:
        if node != old_pr_node:
            assert countDiscarded(node.elector,
                                  'of master in previous view too') == 1

    # The new primary can still process requests
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)

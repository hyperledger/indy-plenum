from plenum.test.pool_transactions.conftest import clientAndWallet1, client1, \
    wallet1, client1Connected, looper
from plenum.test.helper import checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.malicious_behaviors_node import slow_primary
from plenum.test.test_node import getPrimaryReplica, ensureElectionsDone
from plenum.test.view_change.helper import do_vc
from stp_core.common.log import getlogger

logger = getlogger()


def test_master_primary_different_from_previous(txnPoolNodeSet,
                                                 looper, client1,
                                                 wallet1, client1Connected):
    """
    After a view change, primary must be different from previous primary for
    master instance, it does not matter for other instance. The primary is
    benign and does not vote for itself.
    """
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    pr = slow_primary(txnPoolNodeSet, 0, delay=10)
    old_pr_node_name = pr.node.name

    # View change happens
    do_vc(looper, txnPoolNodeSet, client1, wallet1, old_view_no)
    logger.debug("VIEW HAS BEEN CHANGED!")
    # Elections done
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # New primary is not same as old primary
    assert getPrimaryReplica(txnPoolNodeSet, 0).node.name != old_pr_node_name

    pr.outBoxTestStasher.resetDelays()

    # The new primary can still process requests
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)


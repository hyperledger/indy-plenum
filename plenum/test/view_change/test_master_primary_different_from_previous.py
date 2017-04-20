from plenum.test import waits
from plenum.test.delayers import delayNonPrimaries
from plenum.test.helper import checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.malicious_behaviors_node import slow_primary
from plenum.test.test_node import getPrimaryReplica
from plenum.test.pool_transactions.conftest import clientAndWallet1, client1, \
    wallet1, client1Connected, looper
from plenum.test.view_change.helper import chkViewChange
from stp_core.loop.eventually import eventually


def test_master_primary_different_from_previous(pool_with_election_done,
                                                looper, client1, wallet1,
                                                client1Connected):
    """
    After a view change, primary must be different from previous primary for
    master instance, it does not matter for other instance. Break it into
    2 tests, one where the primary is benign and does not vote for itself and
    the other where primary votes for itself and is still not made primary.
    """
    nodes = pool_with_election_done

    old_view_no = checkViewNoForNodes(nodes)
    pr = slow_primary(nodes, 0, delay=10)
    old_pr_node_name = pr.node.name

    # View change happens
    looper.run(eventually(chkViewChange, nodes, old_view_no+1, wallet1, client1,
                          retryWait=1, timeout=60))
    # New primary is not same as old primary
    assert getPrimaryReplica(nodes, 0).node.name != old_pr_node_name

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)


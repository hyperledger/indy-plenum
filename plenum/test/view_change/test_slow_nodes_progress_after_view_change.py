import pytest

from plenum.test.delayers import cDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.view_change.helper import ensure_view_change, \
    elongate_view_change_timeout


@pytest.fixture(scope="module")
def tconf(tconf, request):
    return elongate_view_change_timeout(tconf, request, by=10)


def test_slow_nodes_progress_after_view_change(txnPoolNodeSet, looper,
                                               tconf, client1, wallet1,
                                               client1Connected):
    """
    A node is slow to process COMMITs so that it orders requests much later
    than others, a view change is triggered and all others enter the new view and
    the new primary starts to send new 3PC messages but the slow node eventually
    processes all requests. The way its checked is by comparing the timestamp
    of ordering the older view requests by the slow node and making sure its
    ordered after the view change is done
    :return:
    """
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    slow_rep = nprs[0]
    slow_node = slow_rep.node
    slow_node.nodeIbStasher.delay(cDelay(3, 0))
    ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    waitNodeDataEquality(looper, slow_node,
                         *[n for n in txnPoolNodeSet if n != slow_node])
    # TODO: This throws commit error

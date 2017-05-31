import pytest

from plenum.test.delayers import cDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.test_node import getNonPrimaryReplicas


@pytest.mark.skip()
def test_stash_order_commits_during_election(looper, txnPoolNodeSet, client1,
                                                wallet1, client1Connected):
    """
    2 non primary nodes are slow to receive COMMITs by the same amount, other
    nodes receive commit but since the safe last ordered seq no is lower,
    they stash it. They later process the stashed commits
    """
    # TODO
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)
    delay = 3
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    slow_nodes = [r.node for r in nprs[:2]]
    fast_nodes = [n for n in txnPoolNodeSet if n not in slow_nodes]
    for node in slow_nodes:
        node.nodeIbStasher.delay(cDelay(delay, 0))

from plenum.test.delayers import pDelay

from plenum.test.test_node import get_master_primary_node
from plenum.test.pool_transactions.conftest import looper
from plenum.test.helper import sdk_send_random_and_check


def test_primary_receives_delayed_prepares(looper, txnPoolNodeSet,
                                           sdk_wallet_client,
                                           sdk_pool_handle):
    """
    Primary gets all PREPAREs after COMMITs
    """
    delay = 50
    primary_node = get_master_primary_node(txnPoolNodeSet)
    other_nodes = [n for n in txnPoolNodeSet if n != primary_node]
    primary_node.nodeIbStasher.delay(pDelay(delay, 0))

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              count=10)

    for node in other_nodes:
        assert node.master_replica.prePrepares
        assert node.master_replica.prepares
        assert node.master_replica.commits

    assert primary_node.master_replica.sentPrePrepares
    assert not primary_node.master_replica.prepares
    assert primary_node.master_replica.commits

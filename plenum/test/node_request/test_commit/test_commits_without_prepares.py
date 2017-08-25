from plenum.test.delayers import pDelay
from plenum.test.helper import send_reqs_batches_and_get_suff_replies, \
    send_reqs_to_nodes_and_verify_all_replies
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.test_node import get_master_primary_node


def test_primary_receives_delayed_prepares(looper, txnPoolNodeSet,
                                           client1, wallet1,
                                           client1Connected):
    """
    Primary gets all PREPAREs after COMMITs
    """
    delay = 50
    primary_node = get_master_primary_node(txnPoolNodeSet)
    other_nodes = [n for n in txnPoolNodeSet if n != primary_node]
    primary_node.nodeIbStasher.delay(pDelay(delay, 0))

    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 10)

    for node in other_nodes:
        assert node.master_replica.prePrepares
        assert node.master_replica.prepares
        assert node.master_replica.commits

    assert primary_node.master_replica.sentPrePrepares
    assert not primary_node.master_replica.prepares
    assert primary_node.master_replica.commits

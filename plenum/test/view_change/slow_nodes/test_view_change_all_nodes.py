from plenum.test.view_change.helper import view_change_in_between_3pc
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper


def test_view_change_in_between_3pc_all_nodes(txnPoolNodeSet, looper,
                                              wallet1, client):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet, txnPoolNodeSet, wallet1,
                               client)


def test_view_change_in_between_3pc_all_nodes_long_delay(
        txnPoolNodeSet, looper, wallet1, client):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet,
                               txnPoolNodeSet,
                               wallet1, client,
                               slow_delay=20)

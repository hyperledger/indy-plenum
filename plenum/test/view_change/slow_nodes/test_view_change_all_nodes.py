from plenum.test.view_change.helper import view_change_in_between_3pc


def test_view_change_in_between_3pc_all_nodes(txnPoolNodeSet, looper,
                                              wallet1, client1):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet, txnPoolNodeSet, wallet1,
                               client1)


def test_view_change_in_between_3pc_all_nodes_long_delay(
        txnPoolNodeSet, looper, wallet1, client1):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet,
                               txnPoolNodeSet,
                               wallet1, client1,
                               slow_delay=20)

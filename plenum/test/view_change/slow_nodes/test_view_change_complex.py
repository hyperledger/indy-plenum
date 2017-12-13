from plenum.test.view_change.helper import \
    view_change_in_between_3pc_random_delays
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper


TestRunningTimeLimitSec = 300


def test_view_change_complex(
        txnPoolNodeSet, looper, wallet1, client, tconf):
    """
    - Complex scenario with multiple view changes
    """

    # # 1. check if 2 of 4 nodes (non-primary) are slowed
    # slow_nodes = [replica.node for replica in getNonPrimaryReplicas(txnPoolNodeSet)[1:]]
    # view_change_in_between_3pc(looper, txnPoolNodeSet, slow_nodes, wallet1,
    #                            client)
    #
    # # 2. check if 2 of 4 nodes (including old primary) are slowed
    # slow_nodes = [get_master_primary_node(txnPoolNodeSet),
    #               get_last_master_non_primary_node(txnPoolNodeSet)]
    # view_change_in_between_3pc(looper, txnPoolNodeSet, slow_nodes, wallet1,
    #                            client)
    #
    # # 3. check if 2 of 4 nodes (including new primary) are slowed
    # slow_nodes = [get_first_master_non_primary_node(txnPoolNodeSet),
    #               get_last_master_non_primary_node(txnPoolNodeSet)]
    # view_change_in_between_3pc(looper, txnPoolNodeSet, slow_nodes, wallet1,
    #                            client)
    #
    # # 4. check if all nodes are slowed
    # view_change_in_between_3pc(looper, txnPoolNodeSet, txnPoolNodeSet, wallet1,
    #                            client)

    view_change_in_between_3pc_random_delays(
        looper,
        txnPoolNodeSet,
        txnPoolNodeSet,
        wallet1,
        client,
        tconf,
        min_delay=0,
        max_delay=10)
    view_change_in_between_3pc_random_delays(
        looper,
        txnPoolNodeSet,
        txnPoolNodeSet,
        wallet1,
        client,
        tconf,
        min_delay=1,
        max_delay=5)
    view_change_in_between_3pc_random_delays(
        looper,
        txnPoolNodeSet,
        txnPoolNodeSet,
        wallet1,
        client,
        tconf,
        min_delay=5)

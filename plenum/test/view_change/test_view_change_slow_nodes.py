import pytest
from plenum.test.test_node import getNonPrimaryReplicas, get_master_primary_node, \
    get_last_master_non_primary_node, get_first_master_non_primary_node
from plenum.test.view_change.helper import view_change_in_between_3pc, \
    view_change_in_between_3pc_random_delays
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper


@pytest.fixture(scope="module")
def client(looper, txnPoolNodeSet, client1, client1Connected):
    return client1Connected


def test_view_change_in_between_3pc_2_of_4_nodes_with_non_primary(
        txnPoolNodeSet, looper, wallet1, client):
    """
    - Slow processing 3PC messages for 2 of 4 node (2>f).
    - Both nodes are non-primary for master neither in this nor the next view
    - do view change
    """
    slow_nodes = [replica.node for replica in getNonPrimaryReplicas(txnPoolNodeSet)[1:]]
    view_change_in_between_3pc(looper, txnPoolNodeSet, slow_nodes, wallet1,
                               client)


def test_view_change_in_between_3pc_2_of_4_nodes_with_old_primary(
        txnPoolNodeSet, looper, wallet1, client):
    """
    - Slow processing 3PC messages for 2 of 4 node (2>f)
    - Slow the current Primary node and the last non-primary node (it will not
     be the next primary because of round-robin).
    - do view change
    """
    slow_nodes = [get_master_primary_node(txnPoolNodeSet),
                  get_last_master_non_primary_node(txnPoolNodeSet)]
    view_change_in_between_3pc(looper, txnPoolNodeSet, slow_nodes, wallet1,
                               client)


def test_view_change_in_between_3pc_2_of_4_nodes_with_new_primary(
        txnPoolNodeSet, looper, wallet1, client):
    """
    - Slow processing 3PC messages for 2 of 4 node (2>f)
    - Slow the the first and the last non-primary node
     (the first will be primary and the last one will not be the next primary
      because of round-robin).
    - do view change
    """
    slow_nodes = [get_first_master_non_primary_node(txnPoolNodeSet),
                  get_last_master_non_primary_node(txnPoolNodeSet)]
    view_change_in_between_3pc(looper, txnPoolNodeSet, slow_nodes, wallet1,
                               client)


def test_view_change_in_between_3pc_2_of_4_nodes_with_old_and_new_primary(
        txnPoolNodeSet, looper, wallet1, client1, client):
    """
    - Slow processing 3PC messages for 2 of 4 node (2>f)
    - Slow both current and next primaries
    - do view change
    """
    slow_nodes = [get_master_primary_node(txnPoolNodeSet),
                  get_first_master_non_primary_node(txnPoolNodeSet)]
    view_change_in_between_3pc(looper, txnPoolNodeSet, slow_nodes, wallet1,
                               client)


def test_view_change_in_between_3pc_all_nodes(txnPoolNodeSet, looper,
                                              wallet1, client):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet, txnPoolNodeSet, wallet1,
                               client)


def test_view_change_in_between_3pc_all_nodes_random_delays(txnPoolNodeSet,
                                                            looper, wallet1,
                                                            client):
    """
    - Slow processing 3PC messages for all nodes randomly
    - do view change
    """
    view_change_in_between_3pc_random_delays(looper, txnPoolNodeSet,
                                             txnPoolNodeSet, wallet1, client)
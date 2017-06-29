import pytest

from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.primary_selection.helper import check_newly_added_nodes
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, nodeThetaAdded, \
    stewardAndWallet1, steward1, stewardWallet


@pytest.fixture(scope="module")
def one_node_added(looper, txnPoolNodeSet, nodeThetaAdded):
    # New node knows primary same primary as others and has rank greater
    # than others
    _, _, new_node = nodeThetaAdded
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
    check_newly_added_nodes(looper, txnPoolNodeSet, [new_node])
    return new_node

import pytest

from plenum.common.constants import NODE
from plenum.common.txn_util import get_type
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected, TestNode
from plenum.test.pool_transactions.helper import \
    sdk_add_new_steward_and_node, sdk_pool_refresh



@pytest.fixture(scope="module")
def tconf(tconf, request):
    # Lowering DELTA since some requests will result in validation errors and
    # that will decrease master throughput.
    # TODO: When monitoring metrics are calibrated, these things
    # should be taken care of.
    tconf.DELTA = .6
    return tconf


@pytest.fixture(scope='module')
def sdk_node_theta_added(looper,
                         txnPoolNodeSet,
                         tdir,
                         tconf,
                         sdk_pool_handle,
                         sdk_wallet_steward,
                         allPluginsPath,
                         testNodeClass=TestNode,
                         name=None):
    new_steward_name = "testClientSteward" + randomString(3)
    new_node_name = name or "Theta"
    new_steward_wallet, new_node = \
        sdk_add_new_steward_and_node(looper,
                                     sdk_pool_handle,
                                     sdk_wallet_steward,
                                     new_steward_name,
                                     new_node_name,
                                     tdir,
                                     tconf,
                                     allPluginsPath,
                                     nodeClass=testNodeClass)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)
    return new_steward_wallet, new_node


@pytest.fixture()
def pool_node_txns(poolTxnData):
    node_txns = []
    for txn in poolTxnData["txns"]:
        if get_type(txn) == NODE:
            node_txns.append(txn)
    return node_txns


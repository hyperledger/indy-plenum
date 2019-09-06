import pytest
from stp_core.common.log import getlogger
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test import waits

logger = getlogger()


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 150


def add_new_node(looper, nodes, sdk_pool_handle, sdk_wallet_steward,
                 tdir, tconf, all_plugins_path, name=None):
    node_name = name or randomString(5)
    new_steward_name = "testClientSteward" + randomString(3)
    new_steward_wallet_handle, new_node = \
        sdk_add_new_steward_and_node(looper,
                                     sdk_pool_handle,
                                     sdk_wallet_steward,
                                     new_steward_name,
                                     node_name,
                                     tdir,
                                     tconf,
                                     all_plugins_path)
    nodes.append(new_node)
    looper.run(checkNodesConnected(nodes, customTimeout=60))
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(nodes))
    waitNodeDataEquality(looper, new_node, *nodes[:-1],
                         customTimeout=timeout, exclude_from_check=['check_last_ordered_3pc_backup'])
    return new_node


def test_add_node_with_f_changed(looper, txnPoolNodeSet, tdir, tconf,
                                 allPluginsPath, sdk_pool_handle,
                                 sdk_wallet_steward, limitTestRunningTime):
    nodes = txnPoolNodeSet
    add_new_node(looper,
                 nodes,
                 sdk_pool_handle,
                 sdk_wallet_steward,
                 tdir,
                 tconf,
                 allPluginsPath,
                 name="Node5")
    add_new_node(looper,
                 nodes,
                 sdk_pool_handle,
                 sdk_wallet_steward,
                 tdir,
                 tconf,
                 allPluginsPath,
                 name="Node6")
    add_new_node(looper,
                 nodes,
                 sdk_pool_handle,
                 sdk_wallet_steward,
                 tdir,
                 tconf,
                 allPluginsPath,
                 name="Node7")
    add_new_node(looper,
                 nodes,
                 sdk_pool_handle,
                 sdk_wallet_steward,
                 tdir,
                 tconf,
                 allPluginsPath,
                 name="Node8")
    # check that all nodes have equal number of replica
    assert len(set([n.replicas.num_replicas for n in txnPoolNodeSet])) == 1
    assert txnPoolNodeSet[-1].replicas.num_replicas == txnPoolNodeSet[-1].requiredNumberOfInstances

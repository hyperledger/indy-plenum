import pytest
from stp_core.common.log import getlogger
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test import waits

logger = getlogger()


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 150


@pytest.fixture(scope="module")
def tconf(tconf):
    old_timeout_restricted = tconf.RETRY_TIMEOUT_RESTRICTED
    old_timeout_not_restricted = tconf.RETRY_TIMEOUT_NOT_RESTRICTED
    tconf.RETRY_TIMEOUT_RESTRICTED = 2
    tconf.RETRY_TIMEOUT_NOT_RESTRICTED = 2
    yield tconf

    tconf.RETRY_TIMEOUT_RESTRICTED = old_timeout_restricted
    tconf.RETRY_TIMEOUT_NOT_RESTRICTED = old_timeout_not_restricted


def add_new_node(looper, nodes, steward, steward_wallet,
                 tdir, client_tdir, tconf, all_plugins_path, name=None):
    node_name = name or randomString(5)
    new_steward_name = "testClientSteward" + randomString(3)
    new_steward, new_steward_wallet, new_node = addNewStewardAndNode(looper,
                                                                     steward,
                                                                     steward_wallet,
                                                                     new_steward_name,
                                                                     node_name,
                                                                     tdir,
                                                                     client_tdir,
                                                                     tconf,
                                                                     all_plugins_path)
    nodes.append(new_node)
    looper.run(checkNodesConnected(nodes, customTimeout=60))
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(nodes))
    waitNodeDataEquality(looper, new_node, *nodes[:-1],
                         customTimeout=timeout)
    return new_node


def test_add_node_with_f_changed(looper, txnPoolNodeSet, tdir, tconf,
                                 allPluginsPath, steward1, stewardWallet,
                                 client_tdir, limitTestRunningTime):

    nodes = txnPoolNodeSet
    add_new_node(looper,
                 nodes,
                 steward1,
                 stewardWallet,
                 tdir,
                 client_tdir,
                 tconf,
                 allPluginsPath,
                 name="Node5")
    add_new_node(looper,
                 nodes,
                 steward1,
                 stewardWallet,
                 tdir,
                 client_tdir,
                 tconf,
                 allPluginsPath,
                 name="Node6")
    add_new_node(looper,
                 nodes,
                 steward1,
                 stewardWallet,
                 tdir,
                 client_tdir,
                 tconf,
                 allPluginsPath,
                 name="Node7")
    add_new_node(looper,
                 nodes,
                 steward1,
                 stewardWallet,
                 tdir,
                 client_tdir,
                 tconf,
                 allPluginsPath,
                 name="Node8")
    # check that all nodes have equal number of replica
    assert len(set([n.replicas.num_replicas for n in txnPoolNodeSet])) == 1
    assert txnPoolNodeSet[-1].replicas.num_replicas == txnPoolNodeSet[-1].requiredNumberOfInstances
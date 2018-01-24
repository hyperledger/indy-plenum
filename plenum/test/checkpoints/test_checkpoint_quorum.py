import pytest
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test import waits


@pytest.fixture(scope="module", autouse=True)
def tconf(tconf):
    old_chk_freq = tconf.CHK_FREQ
    old_log_size = tconf.LOG_SIZE
    tconf.CHK_FREQ = 10
    tconf.LOG_SIZE = 30

    yield tconf

    tconf.CHK_FREQ = old_chk_freq
    tconf.LOG_SIZE = old_log_size


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
    looper.run(checkNodesConnected(nodes))
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(nodes))
    waitNodeDataEquality(looper, new_node, *nodes[:-1],
                         customTimeout=timeout)
    return new_node


def test_checkpoint_quorum(looper, txnPoolNodeSet, tdir, tconf,
                        allPluginsPath, steward1, stewardWallet,
                        client_tdir):
    num_requests = 40
    for _ in range(2):
        add_new_node(looper,
                     txnPoolNodeSet,
                     steward1,
                     stewardWallet,
                     tdir,
                     client_tdir,
                     tconf,
                     allPluginsPath)
    sendReqsToNodesAndVerifySuffReplies(looper,
                                        stewardWallet,
                                        steward1,
                                        num_requests,
                                        total_timeout=waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))
    waitNodeDataEquality(looper,
                         txnPoolNodeSet[-1],
                         *txnPoolNodeSet[:-1],
                         customTimeout=waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))


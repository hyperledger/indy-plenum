import psutil
import shutil

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, \
    waitNodeDataEquality

from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.test_node import checkNodesConnected, TestNode
from plenum.common.config_helper import PNodeConfigHelper
from stp_core.types import HA

TestRunningTimeLimitSec = 200


def test_state_regenerated_from_ledger(
        looper,
        txnPoolNodeSet,
        client1,
        wallet1,
        client1Connected,
        tdir,
        tconf,
        allPluginsPath):
    """
    Node loses its state database but recreates it from ledger after start
    """
    sent_batches = 10
    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1,
                                           5 * sent_batches, sent_batches)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    node_to_stop = txnPoolNodeSet[-1]
    node_state = node_to_stop.states[DOMAIN_LEDGER_ID]
    assert not node_state.isEmpty
    state_db_path = node_state._kv.db_path
    nodeHa, nodeCHa = HA(*
                         node_to_stop.nodestack.ha), HA(*
                                                        node_to_stop.clientstack.ha)

    node_to_stop.stop()
    looper.removeProdable(node_to_stop)

    shutil.rmtree(state_db_path)

    config_helper = PNodeConfigHelper(node_to_stop.name, tconf, chroot=tdir)
    restarted_node = TestNode(node_to_stop.name,
                              config_helper=config_helper,
                              config=tconf, ha=nodeHa, cliha=nodeCHa,
                              pluginPaths=allPluginsPath)
    looper.add(restarted_node)
    txnPoolNodeSet[-1] = restarted_node

    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, restarted_node, *txnPoolNodeSet[:-1])


def test_memory_consumption_while_recreating_state_db():
    """
    Check that while re-creating state db from ledger, the node does not read
    all transactions at once. Use psutil to compare memory
    """
    # TODO:
    # p = psutil.Process()
    # print(p.memory_info_ex())

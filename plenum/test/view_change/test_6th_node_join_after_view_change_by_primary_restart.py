import pytest
import types

from plenum.test.view_change.helper import ensure_all_nodes_have_same_data, \
    ensure_view_change_by_primary_restart, start_stopped_node
from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState, POOL_LEDGER_ID
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies

from plenum.test.pool_transactions.conftest import wallet1, client1,\
client1Connected, looper, stewardAndWallet1, steward1, \
     stewardWallet

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.node_catchup.helper import check_ledger_state, \
    waitNodeDataEquality
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test import waits
from plenum.common.startable import Mode


logger = getlogger()


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 600


@pytest.fixture(scope="module", autouse=True)
def tconf(tconf):
    old_vc_timeout = tconf.VIEW_CHANGE_TIMEOUT
    old_max_reconnect_retry = tconf.MAX_RECONNECT_RETRY_ON_SAME_SOCKET
    tconf.MAX_RECONNECT_RETRY_ON_SAME_SOCKET = 0
    tconf.VIEW_CHANGE_TIMEOUT = 15

    yield tconf

    tconf.VIEW_CHANGE_TIMEOUT = old_vc_timeout
    tconf.MAX_RECONNECT_RETRY_ON_SAME_SOCKET = old_max_reconnect_retry


def catchuped(node):
    assert node.mode == Mode.participating


def add_new_node(looper, nodes, steward, steward_wallet,
                tdir, client_tdir, tconf, all_plugins_path, name=None):
    node_name = name or "Psi"
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


def test_6th_node_join_after_view_change_by_master_restart(
         looper, txnPoolNodeSet, tdir, tconf,
         allPluginsPath, steward1, stewardWallet,
         client_tdir, limitTestRunningTime):
    """
    Test steps:
    1. start pool of 4 nodes
    2. force 4 view change by restarting primary node
    3. now primary node must be Alpha, then add new node, named Epsilon
    4. ensure, that Epsilon was added and catch-up done
    5. send some txns
    6. force 4 view change. Now primary node is new added Epsilon
    7. add 6th node and ensure, that new node is catchuped
    """
    pool_of_nodes = txnPoolNodeSet
    for __ in range(4):
        pool_of_nodes = ensure_view_change_by_primary_restart(looper,
                                                                pool_of_nodes,
                                                                tconf,
                                                                tdir,
                                                                allPluginsPath,
                                                                customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
        timeout = waits.expectedPoolCatchupTime(nodeCount=len(pool_of_nodes))
        for node in pool_of_nodes:
            looper.run(eventually(catchuped, node, timeout=2 * timeout))
    ensure_all_nodes_have_same_data(looper, pool_of_nodes, custom_timeout=timeout)
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 5)

    new_epsilon_node = add_new_node(looper,
                                    pool_of_nodes,
                                    steward1,
                                    stewardWallet,
                                    tdir,
                                    client_tdir,
                                    tconf,
                                    allPluginsPath,
                                    name='Epsilon')
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 5)
    """
    check that pool and domain ledgers for new node are in synced state
    """
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(pool_of_nodes))
    for node in pool_of_nodes:
        looper.run(eventually(check_ledger_state, node, DOMAIN_LEDGER_ID,
                              LedgerState.synced, retryWait=.5, timeout=timeout))
        looper.run(eventually(check_ledger_state, node, POOL_LEDGER_ID,
                              LedgerState.synced, retryWait=.5, timeout=timeout))
    for __ in range(4):
        pool_of_nodes = ensure_view_change_by_primary_restart(looper,
                                                               pool_of_nodes,
                                                               tconf,
                                                               tdir,
                                                               allPluginsPath,
                                                               customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)

        timeout = waits.expectedPoolCatchupTime(nodeCount=len(pool_of_nodes))
        for node in pool_of_nodes:
            looper.run(eventually(catchuped, node, timeout=2 * timeout))
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 2)
    new_psi_node = add_new_node(looper,
                                pool_of_nodes,
                                steward1,
                                stewardWallet,
                                tdir,
                                client_tdir,
                                tconf,
                                allPluginsPath,
                                name='Psi')
    looper.run(eventually(check_ledger_state, new_psi_node, DOMAIN_LEDGER_ID,
                          LedgerState.synced, retryWait=.5, timeout=5))
    looper.run(eventually(check_ledger_state, new_psi_node, POOL_LEDGER_ID,
                          LedgerState.synced, retryWait=.5, timeout=5))



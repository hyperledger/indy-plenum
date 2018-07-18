import pytest

from plenum.test.view_change.helper import ensure_all_nodes_have_same_data, \
    ensure_view_change, add_new_node
from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState, POOL_LEDGER_ID
from plenum.test.helper import sdk_send_random_and_check

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.node_catchup.helper import check_ledger_state, \
    waitNodeDataEquality
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node, sdk_pool_refresh
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
    tconf.VIEW_CHANGE_TIMEOUT = 30

    yield tconf

    tconf.VIEW_CHANGE_TIMEOUT = old_vc_timeout
    tconf.MAX_RECONNECT_RETRY_ON_SAME_SOCKET = old_max_reconnect_retry


def catchuped(node):
    assert node.mode == Mode.participating


def test_6th_node_join_after_view_change_by_master_restart(
        looper, txnPoolNodeSet, tdir, tconf,
        allPluginsPath, sdk_pool_handle,
        sdk_wallet_steward,
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
    for __ in range(4):
        ensure_view_change(looper, txnPoolNodeSet, custom_timeout=tconf.VIEW_CHANGE_TIMEOUT)

    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=tconf.VIEW_CHANGE_TIMEOUT)
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(txnPoolNodeSet))
    for node in txnPoolNodeSet:
        looper.run(eventually(catchuped, node, timeout=2 * timeout))
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=timeout)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)

    new_epsilon_node = add_new_node(looper,
                                    txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_steward,
                                    tdir,
                                    tconf,
                                    allPluginsPath,
                                    name='Epsilon')
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)
    """
    check that pool and domain ledgers for new node are in synced state
    """
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(txnPoolNodeSet))
    for node in txnPoolNodeSet:
        looper.run(eventually(check_ledger_state, node, DOMAIN_LEDGER_ID,
                              LedgerState.synced, retryWait=.5, timeout=timeout))
        looper.run(eventually(check_ledger_state, node, POOL_LEDGER_ID,
                              LedgerState.synced, retryWait=.5, timeout=timeout))
    for __ in range(4):
        ensure_view_change(looper, txnPoolNodeSet, custom_timeout=tconf.VIEW_CHANGE_TIMEOUT)

    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=tconf.VIEW_CHANGE_TIMEOUT)
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(txnPoolNodeSet))
    for node in txnPoolNodeSet:
        looper.run(eventually(catchuped, node, timeout=3 * timeout))
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 2)
    new_psi_node = add_new_node(looper,
                                txnPoolNodeSet,
                                sdk_pool_handle,
                                sdk_wallet_steward,
                                tdir,
                                tconf,
                                allPluginsPath,
                                name='Psi')
    looper.run(eventually(check_ledger_state, new_psi_node, DOMAIN_LEDGER_ID,
                          LedgerState.synced, retryWait=.5, timeout=5))
    looper.run(eventually(check_ledger_state, new_psi_node, POOL_LEDGER_ID,
                          LedgerState.synced, retryWait=.5, timeout=5))

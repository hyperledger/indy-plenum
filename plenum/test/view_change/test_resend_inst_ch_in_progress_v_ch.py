import pytest
from stp_core.loop.eventually import eventually

from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.test.stasher import delay_rules_without_processing
from plenum.test.delayers import icDelay
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.view_change.helper import ensure_view_change

from plenum.test.helper import viewNoForNodes

nodeCount = 7


@pytest.fixture(scope='module')
def tconf(tconf):
    old_value_v_ch = tconf.NEW_VIEW_TIMEOUT
    old_value_freshness = tconf.STATE_FRESHNESS_UPDATE_INTERVAL
    tconf.NEW_VIEW_TIMEOUT = 10
    tconf.STATE_FRESHNESS_UPDATE_INTERVAL = 10
    yield tconf
    tconf.NEW_VIEW_TIMEOUT = old_value_v_ch
    tconf.STATE_FRESHNESS_UPDATE_INTERVAL = old_value_freshness


def test_resend_inst_ch_in_progress_v_ch(txnPoolNodeSet, looper, sdk_pool_handle,
                                         sdk_wallet_client, tdir, tconf, allPluginsPath):
    old_view = viewNoForNodes(txnPoolNodeSet)

    # disconnect two nodes. One of them should be next master primary in case of view change.
    for node in [txnPoolNodeSet[1], txnPoolNodeSet[-1]]:
        disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node, stopNode=True)
        looper.removeProdable(node)
        txnPoolNodeSet.remove(node)

    # delay I_CH on every node except last one and initiate view change
    stashers = [n.nodeIbStasher for n in txnPoolNodeSet[:-1]]
    with delay_rules_without_processing(stashers, icDelay(viewNo=2)):
        ensure_view_change(looper, txnPoolNodeSet)
        looper.runFor(tconf.NEW_VIEW_TIMEOUT + 1)

    # checks
    def checks():
        assert all(not node.view_change_in_progress for node in txnPoolNodeSet)
        assert all(node.viewNo == old_view + 2 for node in txnPoolNodeSet)

    looper.run(eventually(checks, timeout=tconf.NEW_VIEW_TIMEOUT * 2.5, retryWait=1))

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)

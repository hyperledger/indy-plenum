from plenum.test import waits
from plenum.test.node_request.helper import vdr_ensure_pool_functional
from plenum.test.restart.helper import get_group, restart_nodes

nodeCount = 7


def test_restart_groups_4_of_7_wp_tm(looper, txnPoolNodeSet, tconf, tdir,
                                     vdr_pool_handle, vdr_wallet_client, allPluginsPath):
    tm = tconf.ToleratePrimaryDisconnection + waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))

    restart_group = get_group(txnPoolNodeSet, 4, include_primary=True)

    restart_nodes(looper, txnPoolNodeSet, restart_group, tconf, tdir, allPluginsPath,
                  after_restart_timeout=tm, start_one_by_one=True)
    vdr_ensure_pool_functional(looper, txnPoolNodeSet, vdr_wallet_client, vdr_pool_handle)

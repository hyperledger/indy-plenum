import pytest

from plenum.test.checkpoints.helper import check_for_nodes, check_stable_checkpoint
from stp_core.common.log import getlogger

from plenum.test.conftest import getValueFromModule
from plenum.test.helper import waitForViewChange, \
    sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import start_stopped_node

from plenum.test.primary_selection.test_recover_more_than_f_failure import \
    stop_primary
from stp_core.loop.eventually import eventually

logger = getlogger()


@pytest.fixture(scope="module")
def tconf(tconf):
    old_val = tconf.ToleratePrimaryDisconnection
    tconf.ToleratePrimaryDisconnection = 1000
    yield tconf
    tconf.ToleratePrimaryDisconnection = old_val


def test_recover_stop_primaries_no_view_change(looper, checkpoint_size, txnPoolNodeSet,
                                               allPluginsPath, tdir, tconf, sdk_pool_handle,
                                               sdk_wallet_steward):
    """
    Test that we can recover after having more than f nodes disconnected:
    - send txns
    - stop current master primary
    - restart current master primary
    - send txns
    """

    active_nodes = list(txnPoolNodeSet)
    assert 4 == len(active_nodes)
    initial_view_no = active_nodes[0].viewNo
    checkpoint_freq = tconf.CHK_FREQ

    logger.info("send at least one checkpoint")
    check_for_nodes(active_nodes, check_stable_checkpoint, 0)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 2 * checkpoint_size)
    # TODO: When stable checkpoint is not deleted it makes sense to check just our last checkpoint
    #  and remove eventually
    looper.run(eventually(check_for_nodes, active_nodes, check_stable_checkpoint, 2 * checkpoint_freq))
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    logger.info("Stop first node (current Primary)")
    stopped_node, active_nodes = stop_primary(looper, active_nodes)

    logger.info("Restart the primary node")
    restarted_node = start_stopped_node(stopped_node, looper, tconf, tdir, allPluginsPath)
    # TODO: Actually I'm not sure that this is a correct behavior. Can we restore stable
    #  checkpoint just from audit ledger or node status db?
    check_for_nodes([restarted_node], check_stable_checkpoint, 0)
    check_for_nodes(active_nodes, check_stable_checkpoint, 2 * checkpoint_freq)
    active_nodes = active_nodes + [restarted_node]

    logger.info("Check that primary selected")
    ensureElectionsDone(looper=looper, nodes=active_nodes,
                        instances_list=range(2), customTimeout=30)
    waitForViewChange(looper, active_nodes, expectedViewNo=0)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes,
                                    exclude_from_check=['check_last_ordered_3pc_backup'])

    logger.info("Check if the pool is able to process requests")
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 10 * checkpoint_size)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes,
                                    exclude_from_check=['check_last_ordered_3pc_backup'])
    looper.run(eventually(check_for_nodes, active_nodes, check_stable_checkpoint, 12 * checkpoint_freq))

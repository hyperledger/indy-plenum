import pytest

from plenum.common.config_util import getConfigOnce
from plenum.test.recorder.helper import reload_modules_for_replay, \
    get_replayable_node_class, create_replayable_node_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data


TestRunningTimeLimitSec = 500

whitelist = ['cannot find remote with name']


@pytest.mark.skip
def test_replay_recorded_msgs(txnPoolNodesLooper,
                              txnPoolNodeSet, some_txns_done, testNodeClass,
                              node_config_helper_class, tconf, tdir,
                              allPluginsPath, tmpdir_factory):
    # Run a pool of nodes with each having a recorder.
    # After some txns, record state, replay each node's recorder on a
    # clean node and check that state matches the initial state

    ensure_all_nodes_have_same_data(txnPoolNodesLooper, txnPoolNodeSet)

    for node in txnPoolNodeSet:
        txnPoolNodesLooper.removeProdable(node)

    for node in txnPoolNodeSet:
        node.stop()

    config = getConfigOnce()

    reload_modules_for_replay(tconf)

    replayable_node_class, basedirpath = get_replayable_node_class(
        tmpdir_factory, tdir, testNodeClass, config)

    print('-------------Replaying now---------------------')

    for node in txnPoolNodeSet:
        create_replayable_node_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                                         node, replayable_node_class,
                                         node_config_helper_class, tconf,
                                         basedirpath, allPluginsPath)

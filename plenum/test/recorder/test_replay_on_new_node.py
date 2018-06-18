import pytest
from plenum.common.config_util import getConfigOnce

from plenum.test.recorder.helper import create_replayable_node_and_check, \
    reload_modules_for_replay, get_replayable_node_class
from plenum.test.node_catchup.conftest import sdk_node_created_after_some_txns, \
    sdk_node_set_with_node_added_after_some_txns, sdk_new_node_caught_up

TestRunningTimeLimitSec = 200


@pytest.mark.skip("Temporary")
def test_replay_on_new_node(txnPoolNodesLooper, txnPoolNodeSet, tconf, tdir,
                            testNodeClass, tmpdir_factory,
                            node_config_helper_class, allPluginsPath,
                            sdk_new_node_caught_up):    # noqa: F811

    new_node = sdk_new_node_caught_up

    for node in txnPoolNodeSet:
        txnPoolNodesLooper.removeProdable(node)
        node.stop()

    config = getConfigOnce()

    reload_modules_for_replay(tconf)

    replayable_node_class, basedirpath = get_replayable_node_class(
        tmpdir_factory, tdir, testNodeClass, config)

    print('-------------Replaying now---------------------')
    create_replayable_node_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                                     new_node, replayable_node_class,
                                     node_config_helper_class, tconf,
                                     basedirpath, allPluginsPath)

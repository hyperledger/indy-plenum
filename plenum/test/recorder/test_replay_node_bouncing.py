import pytest
import time

import pytest

from plenum.common.config_util import getConfigOnce

from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.recorder.helper import reload_modules_for_replay, \
    get_replayable_node_class, create_replayable_node_and_check
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from stp_core.loop.eventually import eventually
from stp_core.types import HA

whitelist = ['cannot find remote with name']


TestRunningTimeLimitSec = 150


@pytest.mark.skip
def test_replay_new_bouncing(txnPoolNodesLooper, txnPoolNodeSet, tconf, tdir,
                             testNodeClass, tmpdir_factory,
                             node_config_helper_class, allPluginsPath,
                             some_txns_done, sdk_pool_handle, sdk_wallet_client):
    alpha = txnPoolNodeSet[0]
    old_view_no = alpha.viewNo
    other_nodes = txnPoolNodeSet[1:]
    stopping_at = time.perf_counter()
    alpha.stop()
    txnPoolNodesLooper.removeProdable(alpha)

    txnPoolNodesLooper.run(eventually(checkViewNoForNodes, other_nodes,
                                      old_view_no + 1, retryWait=1, timeout=30))

    sdk_send_random_and_check(txnPoolNodesLooper, other_nodes,
                              sdk_pool_handle,
                              sdk_wallet_client, 10)
    ensure_all_nodes_have_same_data(txnPoolNodesLooper, other_nodes)

    for node in other_nodes:
        assert alpha.name not in node.nodestack.connecteds

    nodeHa, nodeCHa = HA(*alpha.nodestack.ha), HA(*alpha.clientstack.ha)
    config_helper = PNodeConfigHelper(alpha.name, tconf, chroot=tdir)

    restarted_alpha = testNodeClass(
        alpha.name,
        config_helper=config_helper,
        config=tconf,
        ha=nodeHa,
        cliha=nodeCHa,
        pluginPaths=allPluginsPath)

    txnPoolNodesLooper.add(restarted_alpha)
    txnPoolNodeSet[0] = restarted_alpha

    restarting_at = time.perf_counter()
    print('Stopped for {}'.format(restarting_at - stopping_at))

    sdk_send_random_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client, 10)
    ensure_all_nodes_have_same_data(txnPoolNodesLooper, txnPoolNodeSet)

    for node in txnPoolNodeSet:
        node.stop()
        txnPoolNodesLooper.removeProdable(node)

    config = getConfigOnce()

    reload_modules_for_replay(tconf)

    replayable_node_class, basedirpath = get_replayable_node_class(
        tmpdir_factory, tdir, testNodeClass, config)

    print('-------------Replaying now---------------------')

    create_replayable_node_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                                     restarted_alpha, replayable_node_class,
                                     node_config_helper_class, tconf,
                                     basedirpath, allPluginsPath)

    # For disconnections
    for node in other_nodes:
        create_replayable_node_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                                         node, replayable_node_class,
                                         node_config_helper_class, tconf,
                                         basedirpath, allPluginsPath)

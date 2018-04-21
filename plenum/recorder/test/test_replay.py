# Use `do_post_node_creation` to add recorder to each node and add the recorder to the replayer and
import importlib
import os
import shutil

import pytest

from plenum.common.constants import STEWARD_STRING
from plenum.common.util import randomString
from plenum.recorder.src.replayable_node import ReplayableNode as _ReplayableNode
from plenum.test.helper import sdk_send_random_and_check, create_new_test_node
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_add_new_nym
from plenum.recorder.test.test_node_msgs_recording import some_txns_done


@pytest.fixture(scope="module")
def do_post_node_creation():
    def _post_node_creation(node):
        pass

    return _post_node_creation


TestRunningTimeLimitSec = 350


def test_replay_recorded_msgs(txnPoolNodesLooper, do_post_node_creation,
                              txnPoolNodeSet, some_txns_done, testNodeClass,
                              node_config_helper_class, tconf, tdir,
                              allPluginsPath, tmpdir_factory):
    # Run a pool of nodes with each having a recorder.
    # After some txns, record state, replay each node's recorder on a
    # clean node and check that state matches the initial state

    ensure_all_nodes_have_same_data(txnPoolNodesLooper, txnPoolNodeSet)

    for replaying_node in txnPoolNodeSet:
        # node.stop()
        txnPoolNodesLooper.removeProdable(replaying_node)

    new_node_name = 'Alpha'
    tconf.USE_WITH_STACK = 2
    import stp_zmq.kit_zstack
    import plenum.common.stacks
    import plenum.server.node
    importlib.reload(stp_zmq.kit_zstack)
    importlib.reload(plenum.common.stacks)
    importlib.reload(plenum.server.node)

    basedirpath = tmpdir_factory.mktemp('').strpath
    src_etc_dir = os.path.join(tdir, 'etc')
    src_var_dir = os.path.join(tdir, 'var', 'lib', 'indy')
    trg_etc_dir = os.path.join(basedirpath, 'etc')
    trg_var_dir = os.path.join(basedirpath, 'var', 'lib', 'indy')
    os.makedirs(trg_var_dir, exist_ok=True)
    shutil.copytree(src_etc_dir, trg_etc_dir)
    for file in os.listdir(src_var_dir):
        if file.endswith('.json') or file.endswith('_genesis'):
            shutil.copy(os.path.join(src_var_dir, file), trg_var_dir)

    shutil.copytree(os.path.join(src_var_dir, 'keys'),
                    os.path.join(trg_etc_dir, 'keys'))
    shutil.copytree(os.path.join(src_var_dir, 'plugins'),
                    os.path.join(trg_etc_dir, 'plugins'))

    class ReplayableNode(testNodeClass, _ReplayableNode):
        pass

    replaying_node = create_new_test_node(
        ReplayableNode, node_config_helper_class, new_node_name, tconf, basedirpath,
        allPluginsPath)
    txnPoolNodesLooper.add(replaying_node)
    for node in txnPoolNodeSet:
        assert replaying_node.domainLedger.size < node.domainLedger.size

    replaying_node.nodestack
# Use `do_post_node_creation` to add recorder to each node and add the recorder to the replayer and
import importlib
import json
import os
import shutil
from typing import Dict

import pytest

from plenum.common.constants import STEWARD_STRING, OP_FIELD_NAME, PREPREPARE, \
    BATCH
from plenum.common.types import f
from plenum.common.util import randomString, get_utc_epoch
from plenum.recorder.src.recorder import Recorder
from plenum.recorder.src.replayable_node import ReplayableNode as _ReplayableNode
from plenum.test.helper import sdk_send_random_and_check, create_new_test_node
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_add_new_nym
from plenum.recorder.test.test_node_msgs_recording import some_txns_done
from plenum.test.test_node import TestReplica, TestReplicas


@pytest.fixture(scope="module")
def do_post_node_creation():
    def _post_node_creation(node):
        pass

    return _post_node_creation


def to_bytes(v):
    if not isinstance(v, bytes):
        return v.encode()


TestRunningTimeLimitSec = 350
whitelist = ['cannot find remote with name']


def test_replay_recorded_msgs(txnPoolNodesLooper, do_post_node_creation,
                              txnPoolNodeSet, some_txns_done, testNodeClass,
                              node_config_helper_class, tconf, tdir,
                              allPluginsPath, tmpdir_factory):
    # Run a pool of nodes with each having a recorder.
    # After some txns, record state, replay each node's recorder on a
    # clean node and check that state matches the initial state

    ensure_all_nodes_have_same_data(txnPoolNodesLooper, txnPoolNodeSet)

    for node in txnPoolNodeSet:
        txnPoolNodesLooper.removeProdable(node)

    tconf.USE_WITH_STACK = 2
    import stp_zmq.kit_zstack
    importlib.reload(stp_zmq.kit_zstack)
    import plenum.common.stacks
    importlib.reload(plenum.common.stacks)
    import plenum.server.node
    importlib.reload(plenum.server.node)
    import plenum.test.test_node
    importlib.reload(plenum.test.test_node)

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
                    os.path.join(trg_var_dir, 'keys'))
    shutil.copytree(os.path.join(src_var_dir, 'plugins'),
                    os.path.join(trg_var_dir, 'plugins'))

    testNodeClass._nodeStackClass = plenum.common.stacks.nodeStackClass
    testNodeClass._clientStackClass = plenum.common.stacks.clientStackClass

    class _TestReplica(TestReplica):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.pp_times = {}  # type: Dict[(int, int), int]

        def get_utc_epoch_for_preprepare(self, inst_id, view_no, pp_seq_no):
            if inst_id == 0:
                # Since only master replica's timestamp affects txns and hence merkle roots
                return self.pp_times[(view_no, pp_seq_no)]
            else:
                return self.utc_epoch

    class _TestReplicas(TestReplicas):
        _testReplicaClass = _TestReplica

    class ReplayableNode(testNodeClass, _ReplayableNode):
        _time_diff = None

        def utc_epoch(self) -> int:
            """
            Returns the UTC epoch according to recorder
            """
            return get_utc_epoch() - self._time_diff

        def create_replicas(self, config=None):
            return _TestReplicas(self, self.monitor, config)

    for node in txnPoolNodeSet:
        node.stop()

    new_node_name_prim = txnPoolNodeSet[0].name
    new_node_name_nprim = txnPoolNodeSet[1].name
    replaying_node_prim = create_new_test_node(
        ReplayableNode, node_config_helper_class, new_node_name_prim, tconf,
        basedirpath, allPluginsPath)
    replaying_node_nprim = create_new_test_node(
        ReplayableNode, node_config_helper_class, new_node_name_nprim, tconf,
        basedirpath, allPluginsPath)

    for node in txnPoolNodeSet:
        assert replaying_node_prim.domainLedger.size < node.domainLedger.size
        assert replaying_node_nprim.domainLedger.size < node.domainLedger.size

    print('-------------Replaying Now---------------------')

    replay_and_compare(txnPoolNodesLooper, txnPoolNodeSet[0],
                       replaying_node_prim)
    replay_and_compare(txnPoolNodesLooper, txnPoolNodeSet[1],
                       replaying_node_nprim)


def replay_and_compare(looper, node, replaying_node):
    looper.add(replaying_node)
    node_rec = node.nodestack.recorder
    client_rec = node.clientstack.recorder
    pp_times = {}
    for k, v in node_rec.store.iterator(include_value=True):
        parsed = Recorder.get_parsed(v.decode())
        if parsed[0] == Recorder.OUTGOING_FLAG:
            try:
                msg = json.loads(parsed[1])
                if isinstance(msg, dict) and OP_FIELD_NAME in msg:
                    if msg[OP_FIELD_NAME] == PREPREPARE and msg[f.INST_ID.nm] == 0:
                        pp_times[msg[f.VIEW_NO.nm], msg[f.PP_SEQ_NO.nm]] = msg[f.PP_TIME.nm]
                    elif msg[OP_FIELD_NAME] == BATCH:
                        for m in msg['messages']:
                            try:
                                m = json.loads(m)
                                if m[OP_FIELD_NAME] == PREPREPARE and m[f.INST_ID.nm] == 0:
                                    pp_times[m[f.VIEW_NO.nm], msg[f.PP_SEQ_NO.nm]] = m[f.PP_TIME.nm]
                            except json.JSONDecodeError:
                                continue
                    else:
                        continue
            except json.JSONDecodeError:
                continue

    with open(os.path.join(node_rec.store._db_path, Recorder.RECORDER_METADATA_FILENAME), 'r') as fl:
        d = fl.read()
        start_time = json.loads(d)['start_time']

    replaying_node._time_diff = get_utc_epoch() - start_time
    replaying_node.master_replica.pp_times = pp_times
    node_rec.start_playing()
    client_rec.start_playing()

    while client_rec.is_playing or node_rec.is_playing:
        if node_rec.is_playing:
            val = node_rec.get_next()
            while val:
                if val[0] == Recorder.INCOMING_FLAG:
                    replaying_node.nodestack._verifyAndAppend(to_bytes(val[1]),
                                                              to_bytes(val[2]))
                val = node_rec.get_next()
            looper.run(replaying_node.prod())

        if client_rec.is_playing:
            val = client_rec.get_next()
            while val:
                if val[0] == Recorder.INCOMING_FLAG:
                    replaying_node.clientstack._verifyAndAppend(
                        to_bytes(val[1]), to_bytes(val[2]))
                val = client_rec.get_next()
            looper.run(replaying_node.prod())

    looper.runFor(10)

    for lid in node.ledger_ids:
        l = node.getLedger(lid)
        l_r = replaying_node.getLedger(lid)
        assert l_r.size == l.size, (replaying_node, node)
        assert l_r.root_hash == l.root_hash, (replaying_node, node)
        # s = node.getState(lid)
        # s_r = replaying_node.getState(lid)
        # assert s.committedHeadHash == s_r.committedHeadHash
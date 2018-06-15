import importlib
import json
import os
import sys

from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys

from plenum.recorder.recorder import Recorder

from plenum.recorder.replayable_node import prepare_directory_for_replay, \
    create_replayable_node_class
from plenum.recorder.replayer import get_recorders_from_node_data_dir, \
    prepare_node_for_replay_and_replay
from plenum.test.helper import create_new_test_node
from plenum.test.test_node import TestReplica, TestReplicas
from stp_core.loop.eventually import eventually


def replay_and_compare(looper, node, replaying_node):
    node_rec, client_rec = get_recorders_from_node_data_dir(
        os.path.join(node.node_info_dir, 'data'), replaying_node.name)

    start_times_file = os.path.join(node.ledger_dir, 'start_times')
    with open(start_times_file, 'r') as f:
        start_times = json.loads(f.read())

    replaying_node = prepare_node_for_replay_and_replay(looper,
                                                        replaying_node,
                                                        node_rec, client_rec,
                                                        start_times)

    def chk():
        for lid in node.ledger_ids:
            orig_ledger = node.getLedger(lid)
            replayed_ledger = replaying_node.getLedger(lid)
            assert replayed_ledger.size == orig_ledger.size, (replayed_ledger.size, orig_ledger.size)
            assert replayed_ledger.root_hash == orig_ledger.root_hash, (replayed_ledger.root_hash, orig_ledger.root_hash)

    timeout = 10 + (node.domainLedger.size - replaying_node.domainLedger.size)

    # TODO: use eventually
    looper.run(eventually(chk, timeout=timeout))


def reload_modules_for_replay(conf):
    conf.STACK_COMPANION = 2
    _reload_modules()


def reload_modules_for_recorder(conf):
    conf.STACK_COMPANION = 1
    _reload_modules()
    import plenum.test.conftest
    importlib.reload(plenum.test.conftest)


def _reload_modules():
    import stp_zmq.zstack
    import stp_zmq.kit_zstack
    import plenum.common.stacks
    import plenum.server.node
    import plenum.test.test_node
    _reload_module(stp_zmq.kit_zstack)
    _reload_module(plenum.common.stacks)
    _reload_module(plenum.server.node)
    _reload_module(plenum.test.test_node)


def _reload_module(module):
    # importlib.reload(module)
    if module.__name__ in sys.modules:
        del sys.modules[module.__name__]
        importlib.import_module(module.__name__)


def get_replayable_node_class(tmpdir_factory, tdir, node_class, config):
    basedirpath = tmpdir_factory.mktemp('').strpath

    prepare_directory_for_replay(tdir, basedirpath, config)

    replayable_node_class = create_replayable_node_class(TestReplica,
                                                         TestReplicas,
                                                         node_class)
    return replayable_node_class, basedirpath


def create_replayable_node_and_check(looper, all_nodes, node_to_check,
                                     replayable_node_class,
                                     node_config_helper_class, conf,
                                     basedirpath, allPluginsPath):
    print(
        '-------------Replaying node {} now---------------------'.format(node_to_check))
    new_node_name = node_to_check.name
    replaying_node = create_new_test_node(
        replayable_node_class, node_config_helper_class, new_node_name,
        conf, basedirpath, allPluginsPath, node_ha=node_to_check.nodestack.ha,
        client_ha=node_to_check.clientstack.ha)
    for n in all_nodes:
        assert replaying_node.domainLedger.size < n.domainLedger.size
    replay_and_compare(looper, node_to_check, replaying_node)


def create_recorder_for_test(tmpdir_factory, name):
    storage = KeyValueStorageLeveldbIntKeys(
        tmpdir_factory.mktemp('').strpath, name)
    return Recorder(storage)

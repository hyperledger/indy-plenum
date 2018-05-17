import importlib
import json
import os

from plenum.recorder.src.replayable_node import prepare_directory_for_replay, \
    create_replayable_node_class
from plenum.recorder.src.replayer import patch_replaying_node_for_time, \
    replay_patched_node, get_recorders_from_node_data_dir, \
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

    replaying_node = prepare_node_for_replay_and_replay(looper, replaying_node, node_rec,
                                       client_rec, start_times)

    def chk():
        for lid in node.ledger_ids:
            l = node.getLedger(lid)
            l_r = replaying_node.getLedger(lid)
            assert l_r.size == l.size, (l_r.size, l.size)
            assert l_r.root_hash == l.root_hash, (l_r.root_hash, l.root_hash)

    timeout = 10 + (node.domainLedger.size - replaying_node.domainLedger.size)

    # TODO: use eventually
    looper.run(eventually(chk, timeout=timeout))


def reload_modules_for_replay(conf):
    conf.USE_WITH_STACK = 2
    import stp_zmq.kit_zstack
    importlib.reload(stp_zmq.kit_zstack)
    import plenum.common.stacks
    importlib.reload(plenum.common.stacks)
    import plenum.server.node
    importlib.reload(plenum.server.node)
    import plenum.test.test_node
    importlib.reload(plenum.test.test_node)


def get_replayable_node_class(tmpdir_factory, tdir, node_class):
    basedirpath = tmpdir_factory.mktemp('').strpath

    prepare_directory_for_replay(tdir, basedirpath)

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
        conf, basedirpath, allPluginsPath)
    for n in all_nodes:
        assert replaying_node.domainLedger.size < n.domainLedger.size
    replay_and_compare(looper, node_to_check, replaying_node)

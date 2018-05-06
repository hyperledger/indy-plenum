import os
import shutil
from typing import Dict

import plenum
from plenum.common.util import get_utc_epoch


def create_replayable_node_class(replica_class, replicas_class, node_class):
    node_class._nodeStackClass = plenum.common.stacks.nodeStackClass
    node_class._clientStackClass = plenum.common.stacks.clientStackClass

    class _TestReplica(replica_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.pp_times = {}  # type: Dict[(int, int), int]

        def get_utc_epoch_for_preprepare(self, inst_id, view_no, pp_seq_no):
            if inst_id == 0:
                # Since only master replica's timestamp affects txns and
                # hence merkle roots
                return self.pp_times[(view_no, pp_seq_no)]
            else:
                return self.utc_epoch

    class _TestReplicas(replicas_class):
        _testReplicaClass = _TestReplica

    class ReplayableNode(node_class):
        _time_diff = None

        def utc_epoch(self) -> int:
            """
            Returns the UTC epoch according to recorder
            """
            return get_utc_epoch() - self._time_diff

        def create_replicas(self, config=None):
            return _TestReplicas(self, self.monitor, config)

    return ReplayableNode


def prepare_directory_for_replay(node_basedirpath, replay_dir):
    src_etc_dir = os.path.join(node_basedirpath, 'etc')
    src_var_dir = os.path.join(node_basedirpath, 'var', 'lib', 'indy')
    trg_etc_dir = os.path.join(replay_dir, 'etc')
    trg_var_dir = os.path.join(replay_dir, 'var', 'lib', 'indy')
    os.makedirs(trg_var_dir, exist_ok=True)
    shutil.copytree(src_etc_dir, trg_etc_dir)
    for file in os.listdir(src_var_dir):
        if file.endswith('.json') or file.endswith('_genesis'):
            shutil.copy(os.path.join(src_var_dir, file), trg_var_dir)

    shutil.copytree(os.path.join(src_var_dir, 'keys'),
                    os.path.join(trg_var_dir, 'keys'))
    shutil.copytree(os.path.join(src_var_dir, 'plugins'),
                    os.path.join(trg_var_dir, 'plugins'))

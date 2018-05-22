import os
import shutil
from typing import Dict, List

import plenum
from plenum.common.request import ReqKey
from plenum.common.util import get_utc_epoch


def create_replayable_node_class(replica_class, replicas_class, node_class):
    node_class._nodeStackClass = plenum.common.stacks.nodeStackClass
    node_class._clientStackClass = plenum.common.stacks.clientStackClass

    class _TestReplica(replica_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.sent_pps = {}  # type: Dict[(int, int), List]

        def get_utc_epoch_for_preprepare(self, inst_id, view_no, pp_seq_no):
            if inst_id == 0:
                # Since only master replica's timestamp affects txns and
                # hence merkle roots
                key = (view_no, pp_seq_no)
                return self.sent_pps[key][0] if key in self.sent_pps else None
            else:
                return self.utc_epoch

        def consume_req_queue_for_pre_prepare(self, ledger_id, view_no,
                                              pp_seq_no):
            if self.instId == 0:
                tm = self.get_utc_epoch_for_preprepare(self.instId, view_no,
                                                       pp_seq_no)
                if tm is None:
                    return
                req_ids, discarded = self.sent_pps[(view_no, pp_seq_no)][1:]
                fin_reqs = {}
                for key in req_ids:
                    if key not in self.requestQueues[ledger_id]:
                        return
                    fin_req = self.requests[key].finalised
                    fin_reqs[key] = fin_req

                for key in req_ids:
                    self.requestQueues[ledger_id].remove(key)

                valid_reqs = []
                invalid_reqs = []
                rejects = []

                # Not entirely accurate as in the real execution invalid reqs
                # are interleaved with valid reqs but since invalid reqs are
                # never applied as dynamic validation is a read only
                # operation, it is functionally correct.
                # Can be fixed by capturing the exact order
                for req_id in req_ids:
                    key = tuple(req_id)
                    fin_req = fin_reqs[key]
                    self.processReqDuringBatch(
                        fin_req, tm, valid_reqs, invalid_reqs, rejects)

                return valid_reqs, invalid_reqs, rejects, tm
            else:
                return super().consume_req_queue_for_pre_prepare(ledger_id, view_no,
                                                                 pp_seq_no)

    class _TestReplicas(replicas_class):
        _replica_class = _TestReplica

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

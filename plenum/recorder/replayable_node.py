import os
import shutil
from typing import Dict, List

from plenum.common.config_helper import PConfigHelper
from plenum.common.exceptions import InvalidClientMessageException, UnknownIdentifier
from plenum.common.messages.node_messages import Reject

from plenum.common.util import get_utc_epoch


def create_replayable_node_class(replica_class, replicas_class, node_class):

    class _TestReplica(replica_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.sent_pps = {}  # type: Dict[(int, int), List]

        def get_utc_epoch_for_preprepare(self, inst_id, view_no, pp_seq_no):
            # Since only master replica's timestamp affects txns and
            # hence merkle roots
            key = (view_no, pp_seq_no)
            return self.sent_pps[key][0] if key in self.sent_pps else None

        def consume_req_queue_for_pre_prepare(self, ledger_id, view_no,
                                              pp_seq_no):
            valid_reqs = []
            invalid_reqs = []
            rejects = []

            tm = self.get_utc_epoch_for_preprepare(self.instId, view_no,
                                                   pp_seq_no)
            if tm is None:
                # No time for this PRE-PREPARE since a PRE-PREPARE with
                # (view_no, pp_seq_no) was not sent during normal execution
                return valid_reqs, invalid_reqs, rejects, tm

            req_ids, discarded = self.sent_pps[(view_no, pp_seq_no)][1:]
            fin_reqs = {}
            for key in req_ids:
                if key not in self.requestQueues[ledger_id]:
                    # Request not available yet
                    return valid_reqs, invalid_reqs, rejects, tm
                fin_req = self.requests[key].finalised
                fin_reqs[key] = fin_req

            for key in req_ids:
                self.requestQueues[ledger_id].remove(key)

            # Not entirely accurate as in the real execution invalid reqs
            # are interleaved with valid reqs but since invalid reqs are
            # never applied as dynamic validation is a read only
            # operation, it is functionally correct.
            # Can be fixed by capturing the exact order
            idx = 0
            reqs = []
            invalid_indices = []
            for req_id in req_ids:
                key = req_id
                fin_req = fin_reqs[key]
                try:
                    self.processReqDuringBatch(fin_req,
                                               tm)
                except (InvalidClientMessageException, UnknownIdentifier) as ex:
                    self.logger.warning('{} encountered exception {} while processing {}, '
                                        'will reject'.format(self, ex, fin_req))
                    rejects.append((fin_req.key, Reject(fin_req.identifier, fin_req.reqId, ex)))
                    invalid_indices.append(idx)
                finally:
                    reqs.append(fin_req)
                idx += 1
            return reqs, invalid_indices, rejects, tm

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

        def adjustReplicas(self,
                           old_required_number_of_instances: int,
                           new_required_number_of_instances: int):
            r = super().adjustReplicas(old_required_number_of_instances,
                                       new_required_number_of_instances)
            if r > 0:
                if hasattr(self, 'sent_pps'):
                    new_replicas = [r for inst_id, r in self.replicas
                                    if old_required_number_of_instances <=
                                    inst_id < new_required_number_of_instances]
                    for r in new_replicas:
                        if not hasattr(r, 'sent_pps'):
                            r.sent_pps = self.sent_pps.pop(r.instId, {})
            return r

    return ReplayableNode


def prepare_directory_for_replay(node_basedirpath, replay_dir, config):
    src_etc_dir = PConfigHelper._chroot_if_needed(config.GENERAL_CONFIG_DIR,
                                                  node_basedirpath)
    src_var_dir = PConfigHelper._chroot_if_needed(config.GENESIS_DIR,
                                                  node_basedirpath)
    trg_etc_dir = PConfigHelper._chroot_if_needed(config.GENERAL_CONFIG_DIR,
                                                  replay_dir)
    trg_var_dir = PConfigHelper._chroot_if_needed(config.GENESIS_DIR,
                                                  replay_dir)

    os.makedirs(trg_var_dir, exist_ok=True)
    shutil.copytree(src_etc_dir, trg_etc_dir)
    for file in os.listdir(src_var_dir):
        if file.endswith('.json') or file.endswith('_genesis'):
            shutil.copy(os.path.join(src_var_dir, file), trg_var_dir)

    shutil.copytree(PConfigHelper._chroot_if_needed(config.KEYS_DIR,
                                                    node_basedirpath),
                    PConfigHelper._chroot_if_needed(config.KEYS_DIR,
                                                    replay_dir))
    shutil.copytree(
        PConfigHelper._chroot_if_needed(config.PLUGINS_DIR, node_basedirpath),
        PConfigHelper._chroot_if_needed(config.PLUGINS_DIR,
                                        replay_dir))

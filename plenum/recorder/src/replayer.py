import json
import os
import time
from typing import Dict, Tuple

from plenum.common.constants import OP_FIELD_NAME, PREPREPARE, BATCH, \
    KeyValueStorageType, CLIENT_STACK_SUFFIX
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.recorder.src.combined_recorder import CombinedRecorder
from plenum.recorder.src.recorder import Recorder
from storage.helper import initKeyValueStorageIntKeys
from stp_core.loop.looper import Prodable


# class Replayer(Prodable):
#     # Each node has multiple recorders, one for each `NetworkInterface`.
#     # The Replayer plays each recorder on the node.
#     def __init__(self):
#         # id -> Recorder
#         self.recorders = {} # type: Dict[int, Recorder]
#         # Recorder id -> last played event time
#         self.last_replayed = {}
#
#     def add_recorder(self, recorder: Recorder):
#         self.recorders[id(recorder)] = recorder
#
#     async def prod(self, limit: int=None):
#         # Check if any recorder's event needs to be played
#         c = 0
#         for recorder in self.recorders.values():
#             if recorder.is_playing:
#                 val = recorder.get_next(Recorder.INCOMING_FLAG)
#                 if val:
#                     msg, frm = val
#                     c += 1
#         return c

def to_bytes(v):
    if not isinstance(v, bytes):
        return v.encode()


def get_recorders_from_node_data_dir(node_data_dir, node_name) -> Tuple[Recorder, Recorder]:
    node_rec_path = os.path.join(node_data_dir, node_name, 'recorder')
    client_stack_name = node_name + CLIENT_STACK_SUFFIX
    client_rec_path = os.path.join(node_data_dir, client_stack_name, 'recorder')
    # TODO: Change to rocksdb
    client_rec_kv_store = initKeyValueStorageIntKeys(KeyValueStorageType.Leveldb,
                                                     client_rec_path,
                                                     client_stack_name)
    node_rec_kv_store = initKeyValueStorageIntKeys(
        KeyValueStorageType.Leveldb, node_rec_path, node_name)

    return Recorder(node_rec_kv_store, skip_metadata_write=True), \
           Recorder(client_rec_kv_store, skip_metadata_write=True)


def patch_sent_prepreapres(replaying_node, node_recorder):
    sent_pps = {}
    for k, v in node_recorder.store.iterator(include_value=True):
        parsed = Recorder.get_parsed(v.decode())
        outgoings = Recorder.filter_outgoing(parsed)
        if outgoings:
            for out in outgoings:
                try:
                    msg = json.loads(out[0])
                    if isinstance(msg, dict) and OP_FIELD_NAME in msg:
                        op_name = msg[OP_FIELD_NAME]
                        if op_name == PREPREPARE and msg[f.INST_ID.nm] == 0:
                            v, p = msg[f.VIEW_NO.nm], msg[f.PP_SEQ_NO.nm]
                            sent_pps[v, p] = [msg[f.PP_TIME.nm],
                                              [tuple(l) for l in
                                               msg[f.REQ_IDR.nm]],
                                              msg[f.DISCARDED.nm],
                                              ]
                        elif op_name == BATCH:
                            for m in msg['messages']:
                                try:
                                    m = json.loads(m)
                                    if m[OP_FIELD_NAME] == PREPREPARE and m[
                                        f.INST_ID.nm] == 0:
                                        v, p = m[f.VIEW_NO.nm], m[
                                            f.PP_SEQ_NO.nm]
                                        sent_pps[v, p] = [m[f.PP_TIME.nm],
                                                          [tuple(l) for l in
                                                           m[f.REQ_IDR.nm]],
                                                          m[f.DISCARDED.nm]
                                                          ]
                                except json.JSONDecodeError:
                                    continue
                        else:
                            continue
                except json.JSONDecodeError:
                    continue

    replaying_node.master_replica.sent_pps = sent_pps


def get_combined_recorder(replaying_node, node_recorder, client_recorder):
    kv_store = initKeyValueStorageIntKeys(KeyValueStorageType.Leveldb,
                                          replaying_node.dataLocation,
                                          'combined_recorder')
    cr = CombinedRecorder(kv_store)
    # Always add node recorder first and then client recorder
    cr.add_recorders(node_recorder, client_recorder)
    cr.combine_recorders()
    return cr


def prepare_node_for_replay_and_replay(looper, replaying_node,
                                       node_recorder, client_recorder,
                                       start_times):
    cr = get_combined_recorder(replaying_node, node_recorder, client_recorder)
    cr.start_times = start_times
    patch_replaying_node(replaying_node, node_recorder, start_times)
    return replay_patched_node(looper, replaying_node, cr)


def patch_replaying_node(replaying_node, node_recorder, start_times):
    patch_sent_prepreapres(replaying_node, node_recorder)
    patch_replaying_node_for_time(replaying_node, start_times)


def patch_replaying_node_for_time(replaying_node, start_times):
    node_1st_start_time = start_times[0][0]
    replaying_node._time_diff = get_utc_epoch() - node_1st_start_time


def replay_patched_node(looper, replaying_node, cr):
    node_run_no = 0
    looper.add(replaying_node)
    cr.start_playing()
    next_stop_at = time.perf_counter() + \
                   (cr.start_times[node_run_no][1] - cr.start_times[node_run_no][0])
    while cr.is_playing:
        vals = cr.get_next()
        if next_stop_at is not None and time.perf_counter() >= next_stop_at:
            node_run_no += 1
            if node_run_no < len(cr.start_times):
                sleep_for = cr.start_times[node_run_no][0] - cr.start_times[node_run_no-1][1]
                next_stop_at = time.perf_counter() + (
                        cr.start_times[node_run_no][1] -
                        cr.start_times[node_run_no][0])
                replaying_node.stop()
                looper.removeProdable(replaying_node)
                replaying_node = replaying_node.__class__(replaying_node.name,
                                                          config_helper=replaying_node.config_helper,
                                                          ha=replaying_node.nodestack.ha,
                                                          cliha=replaying_node.clientstack.ha)
                patch_replaying_node_for_time(replaying_node, cr.start_times)
                print('sleeping for {} before starting '.format(sleep_for))
                time.sleep(sleep_for)
                looper.add(replaying_node)
            else:
                next_stop_at = None

        if not vals:
            # looper.runFor(.001)
            continue

        n_msgs, c_msgs = vals
        if n_msgs:
            incomings = Recorder.filter_incoming(n_msgs)
            for inc in incomings:
                replaying_node.nodestack._verifyAndAppend(to_bytes(inc[0]),
                                                          to_bytes(inc[1]))

        if c_msgs:
            incomings = Recorder.filter_incoming(c_msgs)
            for inc in incomings:
                replaying_node.clientstack._verifyAndAppend(to_bytes(inc[0]),
                                                            to_bytes(inc[1]))

        looper.run(replaying_node.prod())

    return replaying_node

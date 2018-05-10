import json
import os
from typing import Dict

from plenum.common.constants import OP_FIELD_NAME, PREPREPARE, BATCH, \
    KeyValueStorageType
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


def patch_replaying_node_for_time(replaying_node, node_recorder):
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
                                              [tuple(l) for l in msg[f.REQ_IDR.nm]],
                                              msg[f.DISCARDED.nm],
                                              ]
                        elif op_name == BATCH:
                            for m in msg['messages']:
                                try:
                                    m = json.loads(m)
                                    if m[OP_FIELD_NAME] == PREPREPARE and m[f.INST_ID.nm] == 0:
                                        v, p = m[f.VIEW_NO.nm], m[f.PP_SEQ_NO.nm]
                                        sent_pps[v, p] = [m[f.PP_TIME.nm],
                                                          [tuple(l) for l in m[f.REQ_IDR.nm]],
                                                          m[f.DISCARDED.nm]
                                                          ]
                                except json.JSONDecodeError:
                                    continue
                        else:
                            continue
                except json.JSONDecodeError:
                    continue

    with open(os.path.join(node_recorder.store._db_path,
                           Recorder.RECORDER_METADATA_FILENAME), 'r') as fl:
        d = fl.read()
        start_time = json.loads(d)['start_time']

    replaying_node._time_diff = get_utc_epoch() - start_time
    replaying_node.master_replica.sent_pps = sent_pps


def replay_patched_node(looper, replaying_node, node_recorder, client_recorder):
    looper.add(replaying_node)
    kv_store = initKeyValueStorageIntKeys(KeyValueStorageType.Leveldb,
                                          replaying_node.dataLocation,
                                          'combined_recorder')
    cr = CombinedRecorder(kv_store, skip_metadata_write=True)
    # Always add node recorder first and then client recorder
    cr.add_recorders(node_recorder, client_recorder)
    cr.combine_recorders()
    cr.start_playing()
    while cr.is_playing:
        vals = cr.get_next()
        if not vals:
            looper.runFor(.001)
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

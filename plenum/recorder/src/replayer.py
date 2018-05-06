import json
import os
from typing import Dict

from plenum.common.constants import OP_FIELD_NAME, PREPREPARE, BATCH
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.recorder.src.recorder import Recorder
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
    pp_times = {}
    for k, v in node_recorder.store.iterator(include_value=True):
        parsed = Recorder.get_parsed(v.decode())
        if parsed[0] == Recorder.OUTGOING_FLAG:
            try:
                msg = json.loads(parsed[1])
                if isinstance(msg, dict) and OP_FIELD_NAME in msg:
                    if msg[OP_FIELD_NAME] == PREPREPARE and msg[f.INST_ID.nm] == 0:
                        pp_times[msg[f.VIEW_NO.nm], msg[f.PP_SEQ_NO.nm]] = msg[
                            f.PP_TIME.nm]
                    elif msg[OP_FIELD_NAME] == BATCH:
                        for m in msg['messages']:
                            try:
                                m = json.loads(m)
                                if m[OP_FIELD_NAME] == PREPREPARE and m[
                                    f.INST_ID.nm] == 0:
                                    v, p = m[f.VIEW_NO.nm], m[f.PP_SEQ_NO.nm]
                                    pp_times[v, p] = m[f.PP_TIME.nm]
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
    replaying_node.master_replica.pp_times = pp_times


def replay_patched_node(looper, replaying_node, node_recorder, client_recorder):
    looper.add(replaying_node)
    node_recorder.start_playing()
    client_recorder.start_playing()

    while client_recorder.is_playing or node_recorder.is_playing:
        if node_recorder.is_playing:
            val = node_recorder.get_next()
            while val:
                if val[0] == Recorder.INCOMING_FLAG:
                    replaying_node.nodestack._verifyAndAppend(to_bytes(val[1]),
                                                              to_bytes(val[2]))
                val = node_recorder.get_next()
            looper.run(replaying_node.prod())

        if client_recorder.is_playing:
            val = client_recorder.get_next()
            while val:
                if val[0] == Recorder.INCOMING_FLAG:
                    replaying_node.clientstack._verifyAndAppend(
                        to_bytes(val[1]), to_bytes(val[2]))
                val = client_recorder.get_next()
            looper.run(replaying_node.prod())


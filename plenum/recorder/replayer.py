import json
import os
import time
from typing import Tuple

from plenum.common.constants import OP_FIELD_NAME, PREPREPARE, BATCH, \
    KeyValueStorageType, CLIENT_STACK_SUFFIX
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.recorder.combined_recorder import CombinedRecorder
from plenum.recorder.recorder import Recorder
from storage.helper import initKeyValueStorageIntKeys


def to_bytes(v):
    if not isinstance(v, bytes):
        return v.encode()


def get_recorders_from_node_data_dir(node_data_dir, node_name) -> Tuple[Recorder, Recorder]:
    rec_path = os.path.join(node_data_dir, node_name, 'recorder')
    client_stack_name = node_name + CLIENT_STACK_SUFFIX
    client_rec_kv_store = initKeyValueStorageIntKeys(KeyValueStorageType.Rocksdb,
                                                     rec_path, client_stack_name)
    node_rec_kv_store = initKeyValueStorageIntKeys(
        KeyValueStorageType.Rocksdb, rec_path, node_name)

    return Recorder(node_rec_kv_store, skip_metadata_write=True), \
        Recorder(client_rec_kv_store, skip_metadata_write=True)


def patch_sent_prepreapres(replaying_node, node_recorder):
    sent_pps = {}

    def add_preprepare(msg):
        inst_id, v, p = msg[f.INST_ID.nm], msg[f.VIEW_NO.nm], msg[
            f.PP_SEQ_NO.nm]
        if inst_id not in sent_pps:
            sent_pps[inst_id] = {}
        sent_pps[inst_id][v, p] = [msg[f.PP_TIME.nm],
                                   [l for l in
                                    msg[f.REQ_IDR.nm]],
                                   msg[f.DISCARDED.nm],
                                   ]

    for k, v in node_recorder.store.iterator(include_value=True):
        parsed = Recorder.get_parsed(v.decode())
        outgoings = Recorder.filter_outgoing(parsed)
        if not outgoings:
            continue
        for out in outgoings:
            try:
                msg = json.loads(out[0])
                if isinstance(msg, dict) and OP_FIELD_NAME in msg:
                    op_name = msg[OP_FIELD_NAME]
                    if op_name == PREPREPARE:
                        add_preprepare(msg)
                    elif op_name == BATCH:
                        for m in msg['messages']:
                            try:
                                m = json.loads(m)
                                if m[OP_FIELD_NAME] == PREPREPARE:
                                    add_preprepare(m)
                            except json.JSONDecodeError:
                                continue
                    else:
                        continue
            except json.JSONDecodeError:
                continue

    for r in replaying_node.replicas:
        r.sent_pps = sent_pps.pop(r.instId, {})

    replaying_node.sent_pps = sent_pps


def get_combined_recorder(replaying_node, node_recorder, client_recorder):
    kv_store = initKeyValueStorageIntKeys(KeyValueStorageType.Rocksdb,
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
    return replay_patched_node(looper, replaying_node, node_recorder, cr)


def patch_replaying_node(replaying_node, node_recorder, start_times):
    patch_sent_prepreapres(replaying_node, node_recorder)
    patch_replaying_node_for_time(replaying_node, start_times)


def patch_replaying_node_for_time(replaying_node, start_times):
    node_1st_start_time = start_times[0][0]
    replaying_node._time_diff = get_utc_epoch() - node_1st_start_time


def replay_patched_node(looper, replaying_node, node_recorder, cr):
    node_run_no = 0
    looper.add(replaying_node)
    cr.start_playing()
    next_stop_at = time.perf_counter() + (cr.start_times[node_run_no][1] -
                                          cr.start_times[node_run_no][0])
    while cr.is_playing:
        vals = cr.get_next()
        if next_stop_at is not None and time.perf_counter() >= next_stop_at:
            node_run_no += 1
            if node_run_no < len(cr.start_times):
                # The node stopped here
                sleep_for = cr.start_times[node_run_no][0] - cr.start_times[node_run_no - 1][1]
                replaying_node.stop()
                looper.removeProdable(replaying_node)
                # Create new node since node is destroyed on stop
                replaying_node = replaying_node.__class__(replaying_node.name,
                                                          config_helper=replaying_node.config_helper,
                                                          ha=replaying_node.nodestack.ha,
                                                          cliha=replaying_node.clientstack.ha)
                patch_replaying_node(replaying_node, node_recorder, cr.start_times)
                print('sleeping for {} to simulate node stop'.format(sleep_for))
                time.sleep(sleep_for)
                after = cr.start_times[node_run_no][1] - cr.start_times[node_run_no][0]
                next_stop_at = time.perf_counter() + after
                print('Next stop after {}'.format(after))
                looper.add(replaying_node)
            else:
                next_stop_at = None

        if not vals:
            continue

        n_msgs, c_msgs = vals
        if n_msgs:
            for inc in n_msgs:
                if Recorder.is_incoming(inc):
                    msg, frm = to_bytes(inc[1]), to_bytes(inc[2])
                    replaying_node.nodestack._verifyAndAppend(msg, frm)
                if Recorder.is_disconn(inc):
                    disconnecteds = inc[1:]
                    replaying_node.nodestack._connsChanged(set(), disconnecteds)

        if c_msgs:
            incomings = Recorder.filter_incoming(c_msgs)
            for inc in incomings:
                msg, frm = to_bytes(inc[0]), to_bytes(inc[1])
                replaying_node.clientstack._verifyAndAppend(msg, frm)

        looper.run(replaying_node.prod())

    return replaying_node

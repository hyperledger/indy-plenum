import os
import time
from typing import Callable

from storage.kv_store_rocksdb_int_keys import KeyValueStorageRocksdbIntKeys

try:
    import ujson as json
except ImportError:
    import json


class Recorder:
    INCOMING_FLAG = 0
    OUTGOING_FLAG = 1
    DISCONN_FLAG = 2
    TIME_FACTOR = 100000000
    RECORDER_METADATA_FILENAME = 'recorder_metadata.json'

    def __init__(self, kv_store: KeyValueStorageRocksdbIntKeys,
                 skip_metadata_write=False):
        self.store = kv_store
        self.replay_targets = {}
        self.is_playing = False
        self.play_started_at = None
        self.store_iterator = None
        self.item_for_next_get = None
        self.last_returned_at = None

    def get_now_key(self):
        return str(int(time.perf_counter() * self.TIME_FACTOR))

    def add_incoming(self, msg, frm, ts: int):
        key, val = self.get_now_key(), self.create_db_val_for_incoming(msg, frm, ts)
        self.add_to_store(key, val)

    def add_outgoing(self, msg, *to):
        key, val = self.get_now_key(), self.create_db_val_for_outgoing(msg, *to)
        self.add_to_store(key, val)

    def add_disconnecteds(self, *names):
        key, val = self.get_now_key(), self.create_db_val_for_disconnecteds(*names)
        self.add_to_store(key, val)

    def add_to_store(self, key, val):
        try:
            existing = self.store.get(key)
            if isinstance(existing, (bytes, bytearray)):
                existing = existing.decode()
            existing = json.loads(existing)
        except KeyError:
            existing = []
        self.store.put(key, json.dumps([*existing, val]))

    def register_replay_target(self, id, target: Callable):
        assert id not in self.replay_targets
        self.replay_targets[id] = target

    @staticmethod
    def create_db_val_for_incoming(msg, frm, ts: int):
        return [Recorder.INCOMING_FLAG, msg, frm, ts]

    @staticmethod
    def create_db_val_for_outgoing(msg, *to):
        return [Recorder.OUTGOING_FLAG, msg, *to]

    @staticmethod
    def create_db_val_for_disconnecteds(*nodes):
        return [Recorder.DISCONN_FLAG, *nodes]

    def start_playing(self):
        assert not self.is_playing
        self.is_playing = True
        self.play_started_at = time.perf_counter()
        self.store_iterator = self.store.iterator(include_value=True)

    def get_next(self):
        if self.item_for_next_get is None:
            try:
                tm, val = next(self.store_iterator)
            except Exception as ex:
                self.is_playing = False
                return None
            tm = int(tm)
            # print(1, tm)
            now = time.perf_counter() * self.TIME_FACTOR
            if self.last_returned_at is None:
                # First item, so return immediately
                self.last_returned_at = (now, tm)
                return self.get_parsed(val)
            else:
                if (now - self.last_returned_at[0]) >= (tm - self.last_returned_at[1]):
                    # Sufficient time has passed
                    self.last_returned_at = (now, tm)
                    return self.get_parsed(val)
                else:
                    # Keep item to return at next get
                    self.item_for_next_get = (tm, val)
        else:
            tm, val = self.item_for_next_get
            # print(2, tm)
            now = time.perf_counter() * self.TIME_FACTOR
            if (now - self.last_returned_at[0]) >= (tm - self.last_returned_at[1]):
                # Sufficient time has passed
                self.last_returned_at = (now, tm)
                self.item_for_next_get = None
                return self.get_parsed(val)

    def stop(self):
        self.store.close()

    @staticmethod
    def get_parsed(msg, only_incoming=None, only_outgoing=None):
        assert not (only_incoming and only_outgoing)
        if isinstance(msg, (bytes, bytearray)):
            msg = msg.decode()
        msg = json.loads(msg)
        if only_incoming:
            return Recorder.filter_incoming(msg)
        if only_outgoing:
            return Recorder.filter_outgoing(msg)
        return msg

    @staticmethod
    def filter_incoming(msgs):
        return [msg[1:] for msg in msgs if Recorder.is_incoming(msg)]

    @staticmethod
    def filter_outgoing(msgs):
        return [msg[1:] for msg in msgs if Recorder.is_outgoing(msg)]

    @staticmethod
    def is_incoming(msg):
        return msg[0] == Recorder.INCOMING_FLAG

    @staticmethod
    def is_outgoing(msg):
        return msg[0] == Recorder.OUTGOING_FLAG

    @staticmethod
    def is_disconn(msg):
        return msg[0] == Recorder.DISCONN_FLAG


def add_start_time(data_directory, tm):
    if not os.path.isdir(data_directory):
        os.makedirs(data_directory)
    file_name = os.path.join(data_directory, 'start_times')
    if not os.path.isfile(file_name):
        start_times = []
    else:
        with open(file_name, 'r') as f:
            start_times = json.loads(f.read())
            if len(start_times[-1]) != 2:
                raise RuntimeError('Wrongly formatted start_times file')
    start_times.append([tm, ])
    with open(file_name, 'w+') as f:
        f.write(json.dumps(start_times))


def add_stop_time(data_directory, tm):
    if not os.path.isdir(data_directory):
        os.makedirs(data_directory)
    file_name = os.path.join(data_directory, 'start_times')
    if not os.path.isfile(file_name):
        raise RuntimeError('No file with start time written')
    else:
        with open(file_name, 'r') as f:
            start_times = json.loads(f.read())
            if len(start_times[-1]) != 1:
                raise RuntimeError('Wrongly formatted start_times file')
    start_times[-1].append(tm)
    with open(file_name, 'w+') as f:
        f.write(json.dumps(start_times))

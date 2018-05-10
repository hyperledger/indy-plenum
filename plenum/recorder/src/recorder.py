import ast
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
        from plenum.common.util import get_utc_epoch
        if not skip_metadata_write:
            with open(os.path.join(kv_store._db_path, self.RECORDER_METADATA_FILENAME), 'w') as f:
                d = {'start_time': get_utc_epoch()}
                f.write(json.dumps(d))

    def get_now_key(self):
        return str(int(time.perf_counter()*self.TIME_FACTOR))

    def add_incoming(self, msg, frm):
        key, val = self.get_now_key(), self.create_db_val_for_incoming(msg, frm)
        self.add_to_store(key, val)

    def add_outgoing(self, msg, *to):
        key, val = self.get_now_key(), self.create_db_val_for_outgoing(msg, *to)
        self.add_to_store(key, val)

    def add_to_store(self, key, val):
        try:
            existing = self.store.get(key)
            existing = json.loads(existing)
        except KeyError:
            existing = []

        self.store.put(key, json.dumps([*existing, val]))

    def register_replay_target(self, id, target: Callable):
        assert id not in self.replay_targets
        self.replay_targets[id] = target

    @staticmethod
    def create_db_val_for_incoming(msg, frm):
        return [Recorder.INCOMING_FLAG, msg, frm]

    @staticmethod
    def create_db_val_for_outgoing(msg, *to):
        return [Recorder.OUTGOING_FLAG, msg, *to]

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
            now = time.perf_counter()*self.TIME_FACTOR
            if self.last_returned_at is None:
                # First item, so return immediately
                self.last_returned_at = (now, tm)
                return self.get_parsed(val)
            else:
                if (now-self.last_returned_at[0]) >= (tm-self.last_returned_at[1]):
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

    def peek_next(self, typ=None):
        # DONT NEED NOW
        assert self.is_playing
        # TODO: Get next key

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
        return [msg[1:] for msg in msgs if msg[0] == Recorder.INCOMING_FLAG]

    @staticmethod
    def filter_outgoing(msgs):
        return [msg[1:] for msg in msgs if msg[0] == Recorder.OUTGOING_FLAG]

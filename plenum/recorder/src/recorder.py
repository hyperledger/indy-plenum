import ast
import time
from typing import Callable

from plenum.common.util import lxor

from storage.kv_store_rocksdb_int_keys import KeyValueStorageRocksdbIntKeys

try:
    import ujson as json
except ImportError:
    import json


class Recorder:
    INCOMING_FLAG = 0
    OUTGOING_FLAG = 1
    TIME_FACTOR = 10000

    def __init__(self, kv_store: KeyValueStorageRocksdbIntKeys):
        self.store = kv_store
        self.replay_targets = {}
        self.is_playing = False
        self.play_started_at = None
        self.store_iterator = None
        self.item_for_next_get = None
        self.last_returned_at = None

    def get_now_key(self):
        return str(int(time.perf_counter()*self.TIME_FACTOR))

    def add_incoming(self, msg, frm):
        self.store.put(self.get_now_key(),
                       self.create_db_val_for_incoming(msg, frm))

    def add_outgoing(self, msg, *to):
        self.store.put(self.get_now_key(),
                       self.create_db_val_for_outgoing(msg, *to))

    def register_replay_target(self, id, target: Callable):
        assert id not in self.replay_targets
        self.replay_targets[id] = target

    @staticmethod
    def create_db_val_for_incoming(msg, frm):
        return json.dumps([Recorder.INCOMING_FLAG, msg, frm])

    @staticmethod
    def create_db_val_for_outgoing(msg, *to):
        return json.dumps([Recorder.OUTGOING_FLAG, msg, *to])

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
        msg = ast.literal_eval(msg)
        if only_incoming:
            return msg[1:] if msg[0] == Recorder.INCOMING_FLAG else None
        if only_outgoing:
            return msg[1:] if msg[0] == Recorder.OUTGOING_FLAG else None
        return msg

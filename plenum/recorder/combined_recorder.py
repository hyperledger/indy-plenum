import time

from plenum.recorder.recorder import Recorder
from storage.kv_store_rocksdb_int_keys import KeyValueStorageRocksdbIntKeys


class CombinedRecorder(Recorder):
    n_prefix = b'\xe2\x82'
    c_prefix = b'\xa0\xa1'
    separator = b'\xc3\x28'

    def __init__(self, kv_store: KeyValueStorageRocksdbIntKeys,
                 skip_metadata_write=True):
        Recorder.__init__(self, kv_store, skip_metadata_write=skip_metadata_write)
        self.recorders = []
        self.start_times = []

    def add_recorders(self, n_recorder, c_recorder):
        # Only 2 recorders
        self.recorders = [n_recorder, c_recorder]

    def combine_recorders(self):
        for k, v in self.recorders[0].store.iterator(include_value=True):
            self.store.put(k, self.n_prefix + v + self.separator)

        for k, v in self.recorders[1].store.iterator(include_value=True):
            try:
                existing = self.store.get(k)
            except KeyError:
                existing = self.n_prefix + self.separator

            self.store.put(k, existing + self.c_prefix + v)

    def start_playing(self):
        assert not self.is_playing
        self.is_playing = True
        self.play_started_at = time.perf_counter()
        self.store_iterator = self.store.iterator(include_value=True)

    @staticmethod
    def get_parsed(msg, only_incoming=None, only_outgoing=None):
        # To conform to the Recorder interface
        assert only_incoming is None
        assert only_outgoing is None

        n_msgs, c_msgs = msg.split(CombinedRecorder.separator)
        n_msgs = n_msgs.lstrip(CombinedRecorder.n_prefix)
        c_msgs = c_msgs.lstrip(CombinedRecorder.c_prefix)

        return [Recorder.get_parsed(n_msgs) if n_msgs else [],
                Recorder.get_parsed(c_msgs) if c_msgs else []]

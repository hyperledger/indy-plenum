import os

from plenum.recorder.src.recorder import Recorder
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys
from stp_zmq.simple_zstack import SimpleZStack


class SimpleZStackWithRecorder(SimpleZStack):
    # Used during recording
    def __init__(self, *args, **kwargs):
        db_path = os.path.join(args[0]['basedirpath'], 'recorder')
        os.makedirs(db_path, exist_ok=True)
        db = KeyValueStorageLeveldbIntKeys(db_path, args[0]['name'])
        self.recorder = Recorder(db)
        super().__init__(*args, **kwargs)

    def _verifyAndAppend(self, msg, ident):
        if super()._verifyAndAppend(msg, ident):
            self.recorder.add_incoming(msg, ident)

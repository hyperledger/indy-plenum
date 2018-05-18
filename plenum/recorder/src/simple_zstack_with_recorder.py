import os
from typing import Set

from stp_core.common.log import getlogger

from plenum.recorder.src.recorder import Recorder
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys
from stp_zmq.simple_zstack import SimpleZStack

logger = getlogger()


class SimpleZStackWithRecorder(SimpleZStack):
    # Used during recording
    def __init__(self, *args, **kwargs):
        parent_dir, _ = os.path.split(args[0]['basedirpath'])
        name = args[0]['name']
        db_path = os.path.join(parent_dir, 'data', name, 'recorder')
        os.makedirs(db_path, exist_ok=True)
        db = KeyValueStorageLeveldbIntKeys(db_path, name)
        self.recorder = Recorder(db)
        super().__init__(*args, **kwargs)

    def _verifyAndAppend(self, msg, ident):
        if super()._verifyAndAppend(msg, ident):
            logger.trace('{} recording incoming {} from {}'.format(self, msg, ident))
            self.recorder.add_incoming(msg, ident)

    def transmit(self, msg, uid, timeout=None, serialized=False):
        status, err = super().transmit(msg, uid, timeout=timeout, serialized=serialized)
        if status:
            self.recorder.add_outgoing(msg, uid)
        return status, err

    def _connsChanged(self, ins: Set[str], outs: Set[str]) -> None:

        from plenum.common.stacks import KITZStack
        if isinstance(self, KITZStack) and outs:
            self.recorder.add_disconnecteds(*outs)
        super()._connsChanged(ins, outs)

    def stop(self):
        self.recorder.stop()
        super().stop()

    # TODO: Not needed as of now
    # def transmitThroughListener(self, msg, ident) -> Tuple[bool, Optional[str]]:
    #     pass
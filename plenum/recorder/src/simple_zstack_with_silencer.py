from typing import Tuple, Optional

from plenum.recorder.src.silencer import Silencer
from stp_zmq.simple_zstack import SimpleZStack


class SimpleZStackWithSilencer(SimpleZStack):
    # Used during replay
    def __init__(self, *args, **kwargs):
        self.silencer = Silencer()
        super().__init__(*args, **kwargs)

    def transmit(self, msg, uid, timeout=None, serialized=False):
        self.silencer.transmit(msg, uid, timeout=timeout, serialized=serialized)

    def transmitThroughListener(self, msg, ident) -> Tuple[bool, Optional[str]]:
        return self.silencer.transmitThroughListener(msg, ident)



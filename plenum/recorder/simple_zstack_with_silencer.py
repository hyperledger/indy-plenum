from typing import Tuple, Optional, Union

from plenum.recorder.silencer import Silencer
from stp_zmq.remote import Remote
from stp_zmq.simple_zstack import SimpleZStack


class Remote_(Remote):
    @property
    def hasLostConnection(self):
        return False


class SimpleZStackWithSilencer(SimpleZStack):
    # Used during replay
    def __init__(self, *args, **kwargs):
        self.silencer = Silencer()
        self._RemoteClass = Remote_
        SimpleZStack.__init__(self, *args, **kwargs)

    def transmit(self, msg, uid, timeout=None, serialized=False):
        self.silencer.transmit(msg, uid, timeout=timeout, serialized=serialized)

    def transmitThroughListener(self, msg, ident) -> Tuple[bool, Optional[str]]:
        return self.silencer.transmitThroughListener(msg, ident)

    def sendPingPong(self, remote: Union[str, Remote], is_ping=True):
        self.silencer.sendPingPong(remote, is_ping=is_ping)

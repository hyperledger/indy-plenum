from typing import Tuple, Optional, Union

from stp_zmq.remote import Remote


class Silencer:
    # During replay, patch the `send` method of each `NetworkInterface` to
    # do nothing.
    def transmit(self, msg, uid, timeout=None, serialized=False, is_batch=False):
        pass

    def transmitThroughListener(self, msg, ident) -> Tuple[bool, Optional[str]]:
        pass

    def sendPingPong(self, remote: Union[str, Remote], is_ping=True):
        pass

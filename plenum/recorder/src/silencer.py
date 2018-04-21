from typing import Tuple, Optional


class Silencer:
    # During replay, patch the `send` method of each `NetworkInterface` to
    # do nothing.
    def transmit(self, msg, uid, timeout=None, serialized=False):
        pass

    def transmitThroughListener(self, msg, ident) -> Tuple[bool, Optional[str]]:
        pass


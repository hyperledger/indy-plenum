from typing import Dict

from plenum.recorder.src.recorder import Recorder
from stp_core.loop.looper import Prodable


class Replayer(Prodable):
    # Each node has multiple recorders, one for each `NetworkInterface`.
    # The Replayer plays each recorder on the node.
    def __init__(self):
        # id -> Recorder
        self.recorders = {} # type: Dict[int, Recorder]
        # Recorder id -> last played event time
        self.last_replayed = {}

    def add_recorder(self, recorder: Recorder):
        self.recorders[id(recorder)] = recorder

    async def prod(self, limit: int=None):
        # Check if any recorder's event needs to be played
        c = 0
        for recorder in self.recorders.values():
            if recorder.is_playing:
                val = recorder.get_next(Recorder.INCOMING_FLAG)
                if val:
                    msg, frm = val
                    c += 1
        return c



from typing import Callable, Iterable
from plenum.server.quorums import Quorums


class NetworkInconsistencyWatcher:
    def __init__(self, cb: Callable):
        self.callback = cb
        self._nodes = set()
        self._connected = set()
        self._quorums = Quorums(0)
        self._reached_consensus = False

    def connect(self, name: str):
        self._connected.add(name)
        if self._quorums.strong.is_reached(len(self._connected)):
            self._reached_consensus = True

    def disconnect(self, name: str):
        self._connected.discard(name)
        if self._reached_consensus and not self._quorums.weak.is_reached(len(self._connected)):
            self._reached_consensus = False
            self.callback()

    @property
    def nodes(self):
        return self._nodes

    def set_nodes(self, nodes: Iterable[str]):
        self._nodes = set(nodes)
        self._quorums = Quorums(len(self._nodes))

    def _has_consensus(self):
        return self._quorums.weak.is_reached(len(self._connected))

from typing import Callable, Iterable
from plenum.server.quorums import Quorums


class NetworkI3PCWatcher:
    def __init__(self, cb: Callable):
        self.nodes = set()
        self.connected = set()
        self.callback = cb
        self.quorums = Quorums(0)

    def connect(self, name: str):
        self.connected.add(name)

    def disconnect(self, name: str):
        had_consensus = self._has_consensus()
        self.connected.discard(name)
        if had_consensus and not self._has_consensus():
            self.callback()

    def set_nodes(self, nodes: Iterable[str]):
        self.nodes = set(nodes)
        self.quorums = Quorums(len(self.nodes))

    def _has_consensus(self):
        return self.quorums.weak.is_reached(len(self.connected))

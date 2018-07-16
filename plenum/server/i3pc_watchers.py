from typing import Callable, Iterable


class NetworkI3PCWatcher:
    def __init__(self, cb: Callable):
        self.nodes = set()
        self.connected = set()
        self.callback = cb

    def connect(self, name: str):
        self.connected.add(name)

    def disconnect(self, name: str):
        had_consensus = self._has_consensus()
        self.connected.discard(name)
        if had_consensus and not self._has_consensus():
            self.callback()

    def set_nodes(self, nodes: Iterable[str]):
        self.nodes = set(nodes)

    def _has_consensus(self):
        n = len(self.nodes)
        f = (n - 1) // 3
        return len(self.connected) > f

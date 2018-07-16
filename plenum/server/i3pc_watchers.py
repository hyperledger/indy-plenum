from typing import Set, Callable


class NetworkI3PCWatcher:
    def __init__(self, nodes: Set[str], cb: Callable):
        self.nodes = nodes
        self.connected = set()
        self.callback = cb

    def connect(self, name: str):
        self.connected.add(name)

    def disconnect(self, name: str):
        had_consensus = self._has_consensus()
        self.connected.discard(name)
        if had_consensus and not self._has_consensus():
            self.callback()

    def add_node(self, name: str):
        self.nodes.add(name)

    def remove_node(self, name: str):
        self.nodes.discard(name)

    def _has_consensus(self):
        n = len(self.nodes)
        f = (n - 1) // 3
        return len(self.connected) > f


from collections import defaultdict
from typing import Tuple


class PoolConnections:
    def __init__(self):
        self._disconnected_till = defaultdict(int)

    def are_connected(self, ts: int, nodes: Tuple[str, str]):
        if nodes[0] == nodes[1]:
            return True
        nodes = self._sort_nodes(nodes)
        return ts >= self._disconnected_till[nodes]

    def disconnect_till(self, ts: int, nodes: Tuple[str, str]):
        nodes = self._sort_nodes(nodes)
        ts = max(ts, self._disconnected_till[nodes])
        self._disconnected_till[nodes] = ts

    @staticmethod
    def _sort_nodes(nodes: Tuple[str, str]):
        if nodes[0] > nodes[1]:
            return (nodes[1], nodes[0])
        return nodes

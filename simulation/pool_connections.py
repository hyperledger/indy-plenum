from collections import defaultdict
from typing import Tuple


class PoolConnections:
    def __init__(self):
        self._disconnected_till = defaultdict(int)

    def are_connected(self, ts: int, node_ids: Tuple[int, int]):
        if node_ids[0] == node_ids[1]:
            return True
        node_ids = self._normalize_ids(node_ids)
        return ts >= self._disconnected_till[node_ids]

    def disconnect_till(self, ts: int, node_ids: Tuple[int, int]):
        node_ids = self._normalize_ids(node_ids)
        ts = max(ts, self._disconnected_till[node_ids])
        self._disconnected_till[node_ids] = ts

    @staticmethod
    def _normalize_ids(ids: Tuple[int, int]):
        if ids[0] > ids[1]:
            return (ids[1], ids[0])
        return ids

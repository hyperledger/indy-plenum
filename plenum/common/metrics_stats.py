from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Sequence, Union

from plenum.common.metrics_collector import MetricsName, KvStoreMetricsFormat
from plenum.common.value_accumulator import ValueAccumulator
from storage.kv_store import KeyValueStorage


def trunc_ts(ts: datetime, step: timedelta):
    base = datetime.min.replace(year=2000)
    step_s = step.total_seconds()
    seconds = (ts - base).total_seconds()
    seconds = int(seconds / step_s) * step_s
    return (base + timedelta(seconds=seconds, milliseconds=500)).replace(microsecond=0)


class MetricsStatsFrame:
    def __init__(self):
        self._stats = defaultdict(ValueAccumulator)

    def add(self, id: MetricsName, value: Union[float, ValueAccumulator]):
        if isinstance(value, ValueAccumulator):
            self._stats[id].merge(value)
        else:
            self._stats[id].add(value)

    def get(self, id: MetricsName) -> ValueAccumulator:
        return self._stats[id]

    def merge(self, other):
        for id, acc in other._stats.items():
            self._stats[id].merge(acc)

    def __eq__(self, other):
        if not isinstance(other, MetricsStatsFrame):
            return False
        for k in set(self._stats.keys()).union(other._stats.keys()):
            if self._stats[k] != other._stats[k]:
                return False
        return True


class MetricsStats:
    def __init__(self, timestep=timedelta(minutes=1)):
        self._timestep = timestep
        self._frames = defaultdict(MetricsStatsFrame)
        self._total = None

    def add(self, ts: datetime, name: MetricsName, value: Union[float, ValueAccumulator]):
        ts = trunc_ts(ts, self._timestep)
        self._frames[ts].add(name, value)
        self._total = None

    def frame(self, ts):
        return self._frames[trunc_ts(ts, self._timestep)]

    def frames(self):
        return self._frames.items()

    @property
    def timestep(self):
        return self._timestep

    @property
    def min_ts(self):
        return min(k for k in self._frames.keys())

    @property
    def max_ts(self):
        return max(k for k in self._frames.keys()) + self._timestep

    @property
    def total(self):
        if self._total is None:
            self._total = self.merge_all(list(self._frames.values()))
        return self._total

    def __eq__(self, other):
        if not isinstance(other, MetricsStats):
            return False
        for k in set(self._frames.keys()).union(other._frames.keys()):
            if self._frames[k] != other._frames[k]:
                return False
        return True

    @staticmethod
    def merge_all(frames: Sequence[MetricsStatsFrame]) -> MetricsStatsFrame:
        count = len(frames)
        if count == 0:
            return MetricsStatsFrame()
        if count == 1:
            return deepcopy(frames[0])

        count_2 = count // 2
        lo = MetricsStats.merge_all(frames[:count_2])
        hi = MetricsStats.merge_all(frames[count_2:])
        lo.merge(hi)
        return lo


def load_metrics_from_kv_store(storage: KeyValueStorage,
                               min_ts: datetime = None,
                               max_ts: datetime = None,
                               step: timedelta = timedelta(minutes=1)) -> MetricsStats:
    result = MetricsStats(step)

    start = KvStoreMetricsFormat.encode_key(min_ts, 0) if min_ts else None
    for k, v in storage.iterator(start=start):
        ev = KvStoreMetricsFormat.decode(k, v)
        if ev is None:
            continue
        if max_ts is not None and ev.timestamp > max_ts:
            break
        result.add(ev.timestamp, ev.name, ev.value)

    return result

import math
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta
from typing import Sequence

from plenum.common.metrics_collector import MetricsName, KvStoreMetricsFormat
from storage.kv_store import KeyValueStorage


def _min_with_none(a, b):
    try:
        return min(a, b)
    except TypeError:
        return a if a is not None else b


def _max_with_none(a, b):
    try:
        return max(a, b)
    except TypeError:
        return a if a is not None else b


def trunc_ts(ts: datetime, step: timedelta):
    base = datetime.min.replace(year=2000)
    step_s = step.total_seconds()
    seconds = (ts - base).total_seconds()
    seconds = int(seconds / step_s) * step_s
    return (base + timedelta(seconds=seconds, milliseconds=500)).replace(microsecond=0)


class ValueAccumulator:
    def __init__(self):
        self._count = 0
        self._sum = 0
        self._sumsq = 0
        self._min = None
        self._max = None

    def add(self, value: float):
        self._count += 1
        self._sum += value
        self._sumsq += value * value
        self._min = _min_with_none(self._min, value)
        self._max = _max_with_none(self._max, value)

    def merge(self, acc):
        self._count += acc._count
        self._sum += acc._sum
        self._sumsq += acc._sumsq
        self._min = _min_with_none(self._min, acc._min)
        self._max = _max_with_none(self._max, acc._max)

    def __repr__(self):
        return "{} samples, {:.2f}/{:.2f}/{:.2f} min/avg/max, {:.2f} stddev". \
            format(self.count, self.min, self.avg, self.max, self.stddev)

    def __eq__(self, other):
        if not isinstance(other, ValueAccumulator):
            return False
        if self._count != other._count:
            return False
        if not math.isclose(self._sum, other._sum):
            return False
        if not math.isclose(self._sumsq, other._sumsq):
            return False
        if self._min != other._min:
            return False
        if self._max != other._max:
            return False
        return True

    @property
    def count(self):
        return self._count

    @property
    def sum(self):
        return self._sum

    @property
    def avg(self):
        return self._sum / self.count if self.count else None

    @property
    def stddev(self):
        if self.count < 2:
            return None
        d = (self._sumsq - self.count * (self.avg ** 2)) / (self.count - 1)
        return math.sqrt(d)

    @property
    def min(self):
        return self._min

    @property
    def max(self):
        return self._max


class MetricsStatsFrame:
    def __init__(self):
        self._stats = defaultdict(ValueAccumulator)

    def add(self, id: MetricsName, value: float):
        self._stats[id].add(value)

    def get(self, id: MetricsName):
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

    def add(self, ts: datetime, name: MetricsName, value: float):
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
            return copy(frames[0])

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

    # TODO: Implement faster filtering by timestamps
    for k, v in storage.iterator():
        ev = KvStoreMetricsFormat.decode(k, v)
        if min_ts is not None and ev.timestamp < min_ts:
            continue
        if max_ts is not None and ev.timestamp > max_ts:
            continue
        result.add(ev.timestamp, ev.name, ev.value)

    return result

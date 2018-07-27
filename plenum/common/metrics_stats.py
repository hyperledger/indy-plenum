import math
from collections import defaultdict

from plenum.common.metrics_collector import MetricsType


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
        self._sumsq += value*value
        self._min = self._min_with_none(self._min, value)
        self._max = self._max_with_none(self._max, value)

    def merge(self, acc):
        self._count += acc._count
        self._sum += acc._sum
        self._sumsq += acc._sumsq
        self._min = self._min_with_none(self._min, acc._min)
        self._max = self._max_with_none(self._max, acc._max)

    def __repr__(self):
        return "{} samples, {}/{}/{} min/avg/max, {} stddev".\
            format(self.count, self.min, self.avg, self.max, self.stddev)

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

    @staticmethod
    def _min_with_none(a, b):
        try:
            return min(a, b)
        except TypeError:
            return a if a is not None else b

    @staticmethod
    def _max_with_none(a, b):
        try:
            return max(a, b)
        except TypeError:
            return a if a is not None else b


class MetricsStats:
    def __init__(self):
        self._stats = defaultdict(ValueAccumulator)

    def add(self, id: MetricsType, value: float):
        self._stats[id].add(value)

    def stats(self, id: MetricsType):
        return self._stats[id]

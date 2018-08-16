import math
import struct
from numbers import Real
from typing import List, Union


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


class ValueAccumulator:
    def __init__(self, value: Union[float, List[float]] = []):
        if isinstance(value, Real):
            self._count = 1
            self._sum = value
            self._sumsq = value * value
            self._min = value
            self._max = value
        else:
            self._count = len(value)
            self._sum = sum(value)
            self._sumsq = sum(v ** 2 for v in value)
            self._min = min(value) if value else None
            self._max = max(value) if value else None

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

    def to_bytes(self) -> bytes:
        if self._count == 1:
            return struct.pack('d', self._sum)
        return struct.pack('dQddd', self._sum, self._count, self._min, self._max, self._sumsq)

    @staticmethod
    def from_bytes(data: bytes):
        if len(data) == 8:
            return ValueAccumulator(struct.unpack('d', data)[0])

        acc = ValueAccumulator()
        acc._sum, acc._count, acc._min, acc._max, acc._sumsq = struct.unpack('dQddd', data)
        return acc

    def __repr__(self):
        if self.count == 0:
            return "no samples"
        return "{} samples, {:.2f} sum, {:.2f}/{:.2f}/{:.2f} min/avg/max, {:.2f} stddev". \
            format(self.count, self.sum, self.min, self.avg, self.max, self.stddev)

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

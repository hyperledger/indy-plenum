import string
from datetime import datetime, timedelta
from numbers import Number
from random import choice, uniform, gauss, randint
from typing import List, Union

from plenum.common import metrics_names
from plenum.common.metrics_collector import MetricsEvent, MetricsStorage
from plenum.common.value_accumulator import ValueAccumulator


def _plenum_metrics():
    result = set(dir(metrics_names))
    result -= {'__builtins__', '__file__', '__cached__', '__doc__',
               '__name__', '__package__', '__loader__', '__spec__'}
    result = [getattr(metrics_names, name) for name in result]
    result = [v for v in result if isinstance(v, bytes)]
    return result


plenum_metrics = _plenum_metrics()


def gen_metrics_name() -> bytes:
    if randint(0, 1):
        return b''.join(choice(string.printable).encode() for _ in range(randint(0, 8)))
    else:
        return choice(plenum_metrics)


def gen_next_timestamp(prev=None) -> datetime:
    def round_ts(ts: datetime) -> datetime:
        us = round(ts.microsecond - 500, -3)
        return ts.replace(microsecond=int(us))

    if prev is None:
        return round_ts(datetime.utcnow())

    return round_ts(prev + timedelta(seconds=uniform(0.001, 10.0)))


def gen_value_accumulator() -> ValueAccumulator:
    return ValueAccumulator([gauss(0.0, 100.0) for _ in range(randint(0, 5))])


def gen_metrics_event(min_ts=None) -> MetricsEvent:
    ts = gen_next_timestamp(min_ts)
    name = gen_metrics_name()
    value = gen_value_accumulator()
    return MetricsEvent(ts, name, value)


def gen_metrics_event_list(num: int, min_ts=None) -> List[MetricsEvent]:
    result = []
    for _ in range(num):
        event = gen_metrics_event(min_ts)
        min_ts = event.timestamp
        result.append(event)
    return result


class MockTimestamp:
    def __init__(self, value=datetime.utcnow()):
        self.value = value

    def __call__(self):
        return self.value


class MockEvent:
    def __init__(self, name, count, sum):
        self.name = name
        self.count = count
        self.sum = sum

    def __eq__(self, other):
        if not isinstance(other, MockEvent):
            return False
        if self.name != other.name:
            return False
        if self.count != other.count:
            return False
        return self.sum == other.sum

    @property
    def avg(self):
        return self.sum / self.count


class MockMetricsStorage(MetricsStorage):
    def __init__(self):
        self.events = []

    def store_event(self, name: str, value: Union[Number, ValueAccumulator]):
        if isinstance(value, Number):
            self.events.append(MockEvent(name, 1, value))
        else:
            self.events.append(MockEvent(name, value.count, value.sum))

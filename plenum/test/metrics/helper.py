from datetime import datetime, timedelta
from random import choice, uniform, gauss, random, randint
from typing import List

from plenum.common.metrics_collector import MetricsName, MetricsEvent, MetricsCollector
from plenum.common.value_accumulator import ValueAccumulator


def gen_metrics_name() -> MetricsName:
    return choice(list(MetricsName))


def gen_next_timestamp(prev=None) -> datetime:
    def round_ts(ts: datetime) -> datetime:
        us = round(ts.microsecond - 500, -3)
        return ts.replace(microsecond=us)

    if prev is None:
        return round_ts(datetime.utcnow())

    return round_ts(prev + timedelta(seconds=uniform(0.001, 10.0)))


def generate_events(num: int, min_ts=None) -> List[MetricsEvent]:
    ts = gen_next_timestamp(min_ts)
    result = []
    for _ in range(num):
        ts = gen_next_timestamp(ts)
        name = gen_metrics_name()
        if random() > 0.5:
            value = gauss(0.0, 100.0)
        else:
            value = ValueAccumulator([gauss(0.0, 100.0) for _ in range(randint(2, 5))])
        result += [MetricsEvent(ts, name, value)]
    return result


class MockTimestamp:
    def __init__(self, value=datetime.utcnow()):
        self.value = value

    def __call__(self):
        return self.value


class MockMetricsCollector(MetricsCollector):
    def __init__(self):
        super().__init__()
        self.events = []

    def add_event(self, name: MetricsName, value: float):
        if isinstance(value, ValueAccumulator):
            value = value.sum
        self.events.append((name, value))

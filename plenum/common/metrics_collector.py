import functools
import struct
import time

from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, NamedTuple, Optional

from plenum.common.value_accumulator import ValueAccumulator
from storage.kv_store import KeyValueStorage


MetricsEvent = NamedTuple('MetricsEvent', [('timestamp', datetime), ('name', bytes), ('value', ValueAccumulator)])


class MetricsStorage(ABC):
    @abstractmethod
    def store_event(self, name: bytes, value: ValueAccumulator):
        pass


class MetricsCollector:
    def __init__(self, storage: Optional[MetricsStorage] = None):
        self.always_accumulate = False
        self._accumulators = defaultdict(ValueAccumulator)
        self._storages = []  # List[MetricsStorage]
        if storage is not None:
            self._storages.append(storage)

    def add_storage(self, storage: MetricsStorage):
        self._storages.append(storage)

    def add_event(self, name: bytes, value: float):
        # Don't do anything unless it's going to be stored someday
        if self.always_accumulate or len(self._storages) > 0:
            self._accumulators[name].add(value)

    def flush_accumulated(self):
        for name, value in self._accumulators.items():
            for storage in self._storages:
                storage.store_event(name, value)
        self._accumulators.clear()

    @contextmanager
    def measure_time(self, name: bytes):
        start = time.perf_counter()
        yield
        self.add_event(name, time.perf_counter() - start)


def measure_time(name: bytes, attr='metrics'):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            metrics = getattr(self, attr)
            with metrics.measure_time(name):
                return f(self, *args, **kwargs)

        return wrapper

    return decorator


def async_measure_time(name: bytes, attr='metrics'):
    def decorator(f):
        @functools.wraps(f)
        async def wrapper(self, *args, **kwargs):
            metrics = getattr(self, attr)
            with metrics.measure_time(name):
                return await f(self, *args, **kwargs)

        return wrapper

    return decorator


class KvStoreMetricsFormat:
    key_bits = 64
    ts_bits = 46
    seq_bits = key_bits - ts_bits
    ts_mask = (1 << ts_bits) - 1
    seq_mask = (1 << seq_bits) - 1

    @staticmethod
    def encode_key(ts: datetime, seq_no: int):
        int_ts = int(1000 * ts.replace(tzinfo=timezone.utc).timestamp())
        int_ts = int_ts & KvStoreMetricsFormat.ts_mask
        seq_no = seq_no & KvStoreMetricsFormat.seq_mask
        return ((int_ts << KvStoreMetricsFormat.seq_bits) | seq_no).to_bytes(8, byteorder='big', signed=False)

    @staticmethod
    def encode(event: MetricsEvent, seq_no: int = 0) -> (bytes, bytes):
        key = KvStoreMetricsFormat.encode_key(event.timestamp, seq_no)
        value = len(event.name).to_bytes(2, byteorder='big', signed=False)
        value += event.name
        value += event.value.to_bytes()
        return key, value

    @staticmethod
    def decode(key: bytes, value: bytes) -> Optional[MetricsEvent]:
        key = int.from_bytes(key, byteorder='big', signed=False)
        ts = datetime.utcfromtimestamp((key >> KvStoreMetricsFormat.seq_bits) / 1000.0)
        name_len = int.from_bytes(value[:2], byteorder='big', signed=False)
        name = bytes(value[2:2 + name_len])
        data = value[2 + name_len:]
        value = ValueAccumulator.from_bytes(data)
        return MetricsEvent(ts, name, value)


class KvStoreMetricsStorage(MetricsStorage):
    def __init__(self, storage: KeyValueStorage, ts_provider: Callable = datetime.utcnow):
        self._storage = storage
        self._ts_provider = ts_provider
        self._seq_no = 0

    def close(self):
        self._storage.close()

    def store_event(self, name: bytes, value: ValueAccumulator):
        if self._storage.closed:
            return

        event = MetricsEvent(self._ts_provider(), name, value)
        key, value = KvStoreMetricsFormat.encode(event, self._seq_no)
        self._storage.put(key, value)
        self._seq_no += 1
        self._seq_no = self._seq_no & KvStoreMetricsFormat.seq_mask

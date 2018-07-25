import struct

from abc import ABC, abstractmethod
from enum import IntEnum
from datetime import datetime, timezone
from typing import Callable

from storage.kv_store import KeyValueStorage


class MetricType(IntEnum):
    NODE_STACK_MESSAGES_PROCESSED = 0
    CLIENT_STACK_MESSAGES_PROCESSED = 1
    LOOPER_RUN_TIME_SPENT = 2
    THREE_PC_BATCH_SIZE = 3
    TRANSPORT_BATCH_SIZE = 4
    OUTGOING_NODE_MESSAGE_SIZE = 5
    INCOMING_NODE_MESSAGE_SIZE = 6
    OUTGOING_CLIENT_MESSAGE_SIZE = 7
    INCOMING_CLIENT_MESSAGE_SIZE = 8


class MetricsCollector(ABC):
    @abstractmethod
    def add_event(self, id: MetricType, value: float):
        pass


class NullMetricsCollector(MetricsCollector):
    def add_event(self, id: MetricType, value: float):
        pass


class KvStoreMetricsCollector(MetricsCollector):
    def __init__(self, storage: KeyValueStorage, ts_provider: Callable = datetime.utcnow):
        self._storage = storage
        self._ts_provider = ts_provider

    def close(self):
        self._storage.close()

    def add_event(self, id: MetricType, value: float):
        if self._storage.closed:
            return

        key = self._encode_key(id, self._ts_provider())
        if key is None:
            return

        value = self._encode_value(value)
        self._storage.put(key, value)

    @staticmethod
    def _encode_key(id: MetricType, timestamp: datetime):
        int_id = int(id)
        if int_id & ((1 << 16) - 1) != int_id:
            return None

        int_time = int(1000 * timestamp.replace(tzinfo=timezone.utc).timestamp())
        if int_time & ((1 << 48) - 1) != int_time:
            return None

        return ((int_id << 48) | int_time).to_bytes(64, byteorder='big', signed=False)

    @staticmethod
    def _encode_value(value: float):
        return struct.pack('d', value)

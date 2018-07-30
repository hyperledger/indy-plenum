import struct

from abc import ABC, abstractmethod
from enum import IntEnum
from datetime import datetime, timezone
from typing import Callable

from storage.kv_store import KeyValueStorage


class MetricsType(IntEnum):
    NODE_STACK_MESSAGES_PROCESSED = 0      # Number of node stack messages processed in one looper run
    CLIENT_STACK_MESSAGES_PROCESSED = 1    # Number of client stack messages processed in one looper run
    LOOPER_RUN_TIME_SPENT = 2              # Seconds passed between looper runs
    THREE_PC_BATCH_SIZE = 3                # Number of requests in one 3PC batch
    TRANSPORT_BATCH_SIZE = 4               # Number of messages in one tranport batch
    OUTGOING_NODE_MESSAGE_SIZE = 5         # Outgoing node message size, bytes
    INCOMING_NODE_MESSAGE_SIZE = 6         # Incoming node message size, bytes
    OUTGOING_CLIENT_MESSAGE_SIZE = 7       # Outgoing client message size, bytes
    INCOMING_CLIENT_MESSAGE_SIZE = 8       # Incoming client message size, bytes


class MetricsCollector(ABC):
    @abstractmethod
    def add_event(self, id: MetricsType, value: float):
        pass


class NullMetricsCollector(MetricsCollector):
    def add_event(self, id: MetricsType, value: float):
        pass


class KvStoreMetricsFormat:
    @staticmethod
    def encode_key(id: MetricsType, timestamp: datetime):
        int_id = int(id)
        if int_id & ((1 << 16) - 1) != int_id:
            return None

        int_time = int(1000 * timestamp.replace(tzinfo=timezone.utc).timestamp())
        if int_time & ((1 << 48) - 1) != int_time:
            return None

        return ((int_id << 48) | int_time).to_bytes(64, byteorder='big', signed=False)

    @staticmethod
    def encode_value(value: float):
        return struct.pack('d', value)

    @staticmethod
    def decode_key(key: bytes) -> (MetricsType, datetime):
        key = int.from_bytes(key, byteorder='big', signed=False)
        id = key >> 48
        ts = key & ((1 << 48) - 1)
        return MetricsType(id), datetime.utcfromtimestamp(ts / 1000)

    @staticmethod
    def decode_value(value: bytes) -> float:
        return struct.unpack('d', value)[0]


class KvStoreMetricsCollector(MetricsCollector):
    def __init__(self, storage: KeyValueStorage, ts_provider: Callable = datetime.utcnow):
        self._storage = storage
        self._ts_provider = ts_provider

    def close(self):
        self._storage.close()

    def add_event(self, id: MetricsType, value: float):
        if self._storage.closed:
            return

        key = KvStoreMetricsFormat.encode_key(id, self._ts_provider())
        if key is None:
            return

        value = KvStoreMetricsFormat.encode_value(value)
        self._storage.put(key, value)

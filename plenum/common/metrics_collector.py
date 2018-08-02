import struct

from abc import ABC, abstractmethod
from enum import IntEnum
from datetime import datetime, timezone
from typing import Callable, NamedTuple

from storage.kv_store import KeyValueStorage


class MetricsName(IntEnum):
    NODE_STACK_MESSAGES_PROCESSED = 0      # Number of node stack messages processed in one looper run
    CLIENT_STACK_MESSAGES_PROCESSED = 1    # Number of client stack messages processed in one looper run
    LOOPER_RUN_TIME_SPENT = 2              # Seconds passed between looper runs
    THREE_PC_BATCH_SIZE = 3                # Number of requests in one 3PC batch
    TRANSPORT_BATCH_SIZE = 4               # Number of messages in one tranport batch
    OUTGOING_NODE_MESSAGE_SIZE = 5         # Outgoing node message size, bytes
    INCOMING_NODE_MESSAGE_SIZE = 6         # Incoming node message size, bytes
    OUTGOING_CLIENT_MESSAGE_SIZE = 7       # Outgoing client message size, bytes
    INCOMING_CLIENT_MESSAGE_SIZE = 8       # Incoming client message size, bytes
    ORDERED_BATCH_SIZE = 9                 # Number of requests ordered
    REQUEST_PROCESSING_TIME = 10           # Time spent on requests processing (including dynamic validation)
    MASTER_3PC_BATCH_SIZE = 11             # Number of requests in one 3PC batch created on master instance
    MASTER_ORDERED_BATCH_SIZE = 12         # Number of requests ordered on master instance
    MASTER_REQUEST_PROCESSING_TIME = 13    # Time spent on requests processing on master instance


MetricsEvent = NamedTuple('MetricsEvent', [('timestamp', datetime), ('name', MetricsName), ('value', float)])


class MetricsCollector(ABC):
    @abstractmethod
    def add_event(self, name: MetricsName, value: float):
        pass


class NullMetricsCollector(MetricsCollector):
    def add_event(self, name: MetricsName, value: float):
        pass


class KvStoreMetricsFormat:
    key_bits = 64
    ts_bits = 53
    seq_bits = key_bits - ts_bits
    ts_mask = (1 << ts_bits) - 1
    seq_mask = (1 << seq_bits) - 1

    @staticmethod
    def encode_key(ts: datetime, seq_no: int):
        int_ts = int(1000000 * ts.replace(tzinfo=timezone.utc).timestamp())
        int_ts = int_ts & KvStoreMetricsFormat.ts_mask
        seq_no = seq_no & KvStoreMetricsFormat.seq_mask
        return ((int_ts << KvStoreMetricsFormat.seq_bits) | seq_no).to_bytes(64, byteorder='big', signed=False)

    @staticmethod
    def encode(event: MetricsEvent, seq_no: int = 0) -> (bytes, bytes):
        key = KvStoreMetricsFormat.encode_key(event.timestamp, seq_no)
        value = event.name.to_bytes(32, byteorder='big', signed=False) + struct.pack('d', event.value)
        return key, value

    @staticmethod
    def decode(key: bytes, value: bytes) -> MetricsEvent:
        key = int.from_bytes(key, byteorder='big', signed=False)
        ts = datetime.utcfromtimestamp((key >> KvStoreMetricsFormat.seq_bits) / 1000000.0)
        name = MetricsName(int.from_bytes(value[:32], byteorder='big', signed=False))
        value = struct.unpack('d', value[32:])[0]
        return MetricsEvent(ts, name, value)


class KvStoreMetricsCollector(MetricsCollector):
    def __init__(self, storage: KeyValueStorage, ts_provider: Callable = datetime.utcnow):
        self._storage = storage
        self._ts_provider = ts_provider
        self._seq_no = 0

    def close(self):
        self._storage.close()

    def add_event(self, name: MetricsName, value: float):
        if self._storage.closed:
            return

        event = MetricsEvent(self._ts_provider(), name, value)
        key, value = KvStoreMetricsFormat.encode(event, self._seq_no)
        self._storage.put(key, value)
        self._seq_no += 1
        self._seq_no = self._seq_no & KvStoreMetricsFormat.seq_mask

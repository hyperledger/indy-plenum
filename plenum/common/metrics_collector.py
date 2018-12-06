import functools
import struct
import time

from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from enum import IntEnum, unique
from datetime import datetime, timezone
from typing import Callable, NamedTuple, Union, Optional

from plenum.common.value_accumulator import ValueAccumulator
from storage.kv_store import KeyValueStorage

TMP_METRIC = 30000


@unique
class MetricsName(IntEnum):
    # Number of node stack messages processed in one looper run
    NODE_STACK_MESSAGES_PROCESSED = 0
    # Number of client stack messages processed in one looper run
    CLIENT_STACK_MESSAGES_PROCESSED = 1
    # Seconds passed between looper runs
    LOOPER_RUN_TIME_SPENT = 2
    # Number of requests in one 3PC batch
    BACKUP_THREE_PC_BATCH_SIZE = 3
    # Number of messages in one tranport batch
    TRANSPORT_BATCH_SIZE = 4
    # Outgoing node message size, bytes
    OUTGOING_NODE_MESSAGE_SIZE = 5
    # Incoming node message size, bytes
    INCOMING_NODE_MESSAGE_SIZE = 6
    # Outgoing client message size, bytes
    OUTGOING_CLIENT_MESSAGE_SIZE = 7
    # Incoming client message size, bytes
    INCOMING_CLIENT_MESSAGE_SIZE = 8
    # Number of requests ordered
    BACKUP_ORDERED_BATCH_SIZE = 9
    # Time spent on requests processing on backup instances
    BACKUP_REQUEST_PROCESSING_TIME = 10
    # Number of requests in one 3PC batch created on master instance
    THREE_PC_BATCH_SIZE = 11
    # Number of requests ordered on master instance
    ORDERED_BATCH_SIZE = 12
    # Time spent on requests processing on master instance
    REQUEST_PROCESSING_TIME = 13
    # Number of invalid request for master
    ORDERED_BATCH_INVALID_COUNT = 14
    # Number of txns sent through catchup
    CATCHUP_TXNS_SENT = 15
    # Number of txns received through catchup
    CATCHUP_TXNS_RECEIVED = 16

    # Average throughput measured by monitor on backup instances
    BACKUP_MONITOR_AVG_THROUGHPUT = 20
    # Average latency measured by monitor on backup instances
    BACKUP_MONITOR_AVG_LATENCY = 21
    # Average throughput measured by monitor on master instance
    MONITOR_AVG_THROUGHPUT = 22
    # Average latency measured by monitor on master instance
    MONITOR_AVG_LATENCY = 23

    # Node incoming request queue size
    REQUEST_QUEUE_SIZE = 30
    FINALISED_REQUEST_QUEUE_SIZE = 31
    MONITOR_REQUEST_QUEUE_SIZE = 32
    MONITOR_UNORDERED_REQUEST_QUEUE_SIZE = 33

    # System statistics
    AVAILABLE_RAM_SIZE = 50
    NODE_RSS_SIZE = 51
    NODE_VMS_SIZE = 52
    CONNECTED_CLIENTS_NUM = 53
    GC_TRACKED_OBJECTS = 54
    GC_GEN0_TIME = 55
    GC_GEN1_TIME = 56
    GC_GEN2_TIME = 57
    GC_UNCOLLECTABLE_OBJECTS = 58
    GC_TOTAL_COLLECTED_OBJECTS = 59
    GC_GEN0_COLLECTED_OBJECTS = 60
    GC_GEN1_COLLECTED_OBJECTS = 61
    GC_GEN2_COLLECTED_OBJECTS = 62
    CURRENT_VIEW = 63
    VIEW_CHANGE_IN_PROGRESS = 64
    NODE_STATUS = 65
    CONNECTED_NODES_NUM = 66
    BLACKLISTED_NODES_NUM = 67
    REPLICA_COUNT = 68
    POOL_LEDGER_SIZE = 69
    DOMAIN_LEDGER_SIZE = 70
    CONFIG_LEDGER_SIZE = 71
    POOL_LEDGER_UNCOMMITTED_SIZE = 72
    DOMAIN_LEDGER_UNCOMMITTED_SIZE = 73
    CONFIG_LEDGER_UNCOMMITTED_SIZE = 74

    # Node service statistics
    NODE_PROD_TIME = 100
    SERVICE_REPLICAS_TIME = 101
    SERVICE_NODE_MSGS_TIME = 102
    SERVICE_CLIENT_MSGS_TIME = 103
    SERVICE_NODE_ACTIONS_TIME = 104
    SERVICE_LEDGER_MANAGER_TIME = 105
    SERVICE_VIEW_CHANGER_TIME = 106
    SERVICE_OBSERVABLE_TIME = 107
    SERVICE_OBSERVER_TIME = 108
    FLUSH_OUTBOXES_TIME = 109
    SERVICE_NODE_LIFECYCLE_TIME = 110
    SERVICE_CLIENT_STACK_TIME = 111
    SERVICE_MONITOR_ACTIONS_TIME = 112

    # Node specific metrics
    SERVICE_NODE_STACK_TIME = 200
    PROCESS_NODE_INBOX_TIME = 201
    SEND_TO_REPLICA_TIME = 202
    NODE_CHECK_PERFORMANCE_TIME = 203
    NODE_CHECK_NODE_REQUEST_SPIKE = 204
    UNPACK_BATCH_TIME = 205
    VERIFY_SIGNATURE_TIME = 207
    SERVICE_REPLICAS_OUTBOX_TIME = 208
    NODE_SEND_TIME = 209
    NODE_SEND_REJECT_TIME = 210
    VALIDATE_NODE_MSG_TIME = 211
    INT_VALIDATE_NODE_MSG_TIME = 212
    PROCESS_ORDERED_TIME = 213
    MONITOR_REQUEST_ORDERED_TIME = 214
    EXECUTE_BATCH_TIME = 215

    # Replica specific metrics
    SERVICE_REPLICA_QUEUES_TIME = 300
    SERVICE_BACKUP_REPLICAS_QUEUES_TIME = 301

    # Master replica message statistics
    PROCESS_PREPREPARE_TIME = 1000
    PROCESS_PREPARE_TIME = 1001
    PROCESS_COMMIT_TIME = 1002
    PROCESS_CHECKPOINT_TIME = 1003
    SEND_PREPREPARE_TIME = 1500
    SEND_PREPARE_TIME = 1501
    SEND_COMMIT_TIME = 1502
    SEND_CHECKPOINT_TIME = 1503
    CREATE_3PC_BATCH_TIME = 1600
    ORDER_3PC_BATCH_TIME = 1601

    # Backup replica message statistics
    BACKUP_PROCESS_PREPREPARE_TIME = 2000
    BACKUP_PROCESS_PREPARE_TIME = 2001
    BACKUP_PROCESS_COMMIT_TIME = 2002
    BACKUP_PROCESS_CHECKPOINT_TIME = 2003
    BACKUP_SEND_PREPREPARE_TIME = 2500
    BACKUP_SEND_PREPARE_TIME = 2501
    BACKUP_SEND_COMMIT_TIME = 2502
    BACKUP_SEND_CHECKPOINT_TIME = 2503
    BACKUP_CREATE_3PC_BATCH_TIME = 2600
    BACKUP_ORDER_3PC_BATCH_TIME = 2601

    # Node message statistics
    PROCESS_PROPAGATE_TIME = 3000
    PROCESS_MESSAGE_REQ_TIME = 3001
    PROCESS_MESSAGE_REP_TIME = 3002
    PROCESS_LEDGER_STATUS_TIME = 3003
    PROCESS_CONSISTENCY_PROOF_TIME = 3004
    PROCESS_CATCHUP_REQ_TIME = 3005
    PROCESS_CATCHUP_REP_TIME = 3006
    PROCESS_REQUEST_TIME = 3100
    SEND_PROPAGATE_TIME = 3500
    SEND_MESSAGE_REQ_TIME = 3501
    SEND_MESSAGE_REP_TIME = 3502

    # BLS statistics
    BLS_VALIDATE_PREPREPARE_TIME = 4000
    BLS_VALIDATE_COMMIT_TIME = 4002
    BLS_UPDATE_PREPREPARE_TIME = 4010
    BLS_UPDATE_COMMIT_TIME = 4012

    # Obsolete metrics
    DESERIALIZE_DURING_UNPACK_TIME = 206

    # Collections metrics for node
    NODE_STACK_RX_MSGS = TMP_METRIC + 1
    CLIENT_STACK_RX_MSGS = TMP_METRIC + 2

    NODE_ACTION_QUEUE = TMP_METRIC + 3
    NODE_AQ_STASH = TMP_METRIC + 4
    NODE_REPEATING_ACTIONS = TMP_METRIC + 5
    NODE_SCHEDULED = TMP_METRIC + 6

    NODE_REQUESTED_PROPAGATES_FOR = TMP_METRIC + 7

    VIEW_CHANGER_ACTION_QUEUE = TMP_METRIC + 8
    VIEW_CHANGER_AQ_STASH = TMP_METRIC + 9
    VIEW_CHANGER_REPEATING_ACTIONS = TMP_METRIC + 10
    VIEW_CHANGER_SCHEDULED = TMP_METRIC + 11
    VIEW_CHANGER_INBOX = TMP_METRIC + 12
    VIEW_CHANGER_OUTBOX = TMP_METRIC + 13
    VIEW_CHANGER_NEXT_VIEW_INDICATIONS = TMP_METRIC + 14
    VIEW_CHANGER_VIEW_CHANGE_DONE = TMP_METRIC + 15

    PRIMARY_DECIDER_ACTION_QUEUE = TMP_METRIC + 16
    PRIMARY_DECIDER_AQ_STASH = TMP_METRIC + 17
    PRIMARY_DECIDER_REPEATING_ACTIONS = TMP_METRIC + 18
    PRIMARY_DECIDER_SCHEDULED = TMP_METRIC + 19
    PRIMARY_DECIDER_INBOX = TMP_METRIC + 20
    PRIMARY_DECIDER_OUTBOX = TMP_METRIC + 21

    MSGS_FOR_FUTURE_REPLICAS = TMP_METRIC + 25
    MSGS_TO_VIEW_CHANGER = TMP_METRIC + 26

    REQUEST_SENDER = TMP_METRIC + 27

    STASHED_ORDERED_REQS = TMP_METRIC + 28

    MSGS_FOR_FUTURE_VIEWS = TMP_METRIC + 29

    TXN_SEQ_RANGE_TO_3PHASE_KEY = TMP_METRIC + 30

    LEDGERMANAGER_POOL_UNCOMMITEDS = TMP_METRIC + 32
    LEDGERMANAGER_DOMAIN_UNCOMMITEDS = TMP_METRIC + 33
    LEDGERMANAGER_CONFIG_UNCOMMITEDS = TMP_METRIC + 34

    # Collections metrics for master replica
    REPLICA_OUTBOX_MASTER = TMP_METRIC + 1001
    REPLICA_INBOX_MASTER = TMP_METRIC + 1002
    REPLICA_INBOX_STASH_MASTER = TMP_METRIC + 1003
    REPLICA_POST_ELECTION_MSGS_MASTER = TMP_METRIC + 1004
    REPLICA_PREPREPARES_PENDING_FIN_REQS_MASTER = TMP_METRIC + 1005
    REPLICA_PREPREPARES_PENDING_PREVPP_MASTER = TMP_METRIC + 1006
    REPLICA_PREPARES_WAITING_FOR_PREPREPARE_MASTER = TMP_METRIC + 1007
    REPLICA_COMMITS_WAITING_FOR_PREPARE_MASTER = TMP_METRIC + 1008
    REPLICA_SENT_PREPREPARES_MASTER = TMP_METRIC + 1009
    REPLICA_PREPREPARES_MASTER = TMP_METRIC + 1010
    REPLICA_PREPARES_MASTER = TMP_METRIC + 1011
    REPLICA_COMMITS_MASTER = TMP_METRIC + 1012
    REPLICA_PRIMARYNAMES_MASTER = TMP_METRIC + 1014
    REPLICA_STASHED_OUT_OF_ORDER_COMMITS_MASTER = TMP_METRIC + 1015
    REPLICA_CHECKPOINTS_MASTER = TMP_METRIC + 1016
    REPLICA_STASHED_RECVD_CHECKPOINTS_MASTER = TMP_METRIC + 1017
    REPLICA_STASHING_WHILE_OUTSIDE_WATERMARKS_MASTER = TMP_METRIC + 1018
    REPLICA_REQUEST_QUEUES_MASTER = TMP_METRIC + 1019
    REPLICA_BATCHES_MASTER = TMP_METRIC + 1020
    REPLICA_REQUESTED_PRE_PREPARES_MASTER = TMP_METRIC + 1021
    REPLICA_REQUESTED_PREPARES_MASTER = TMP_METRIC + 1022
    REPLICA_REQUESTED_COMMITS_MASTER = TMP_METRIC + 1023
    REPLICA_PRE_PREPARES_STASHED_FOR_INCORRECT_TIME_MASTER = TMP_METRIC + 1024

    REPLICA_ACTION_QUEUE_MASTER = TMP_METRIC + 1025
    REPLICA_AQ_STASH_MASTER = TMP_METRIC + 1026
    REPLICA_REPEATING_ACTIONS_MASTER = TMP_METRIC + 1027
    REPLICA_SCHEDULED_MASTER = TMP_METRIC + 1028

    # Collections metrics for backup replica
    REPLICA_OUTBOX_BACKUP = TMP_METRIC + 2001
    REPLICA_INBOX_BACKUP = TMP_METRIC + 2002
    REPLICA_INBOX_STASH_BACKUP = TMP_METRIC + 2003
    REPLICA_POST_ELECTION_MSGS_BACKUP = TMP_METRIC + 2004
    REPLICA_PREPREPARES_PENDING_FIN_REQS_BACKUP = TMP_METRIC + 2005
    REPLICA_PREPREPARES_PENDING_PREVPP_BACKUP = TMP_METRIC + 2006
    REPLICA_PREPARES_WAITING_FOR_PREPREPARE_BACKUP = TMP_METRIC + 2007
    REPLICA_COMMITS_WAITING_FOR_PREPARE_BACKUP = TMP_METRIC + 2008
    REPLICA_SENT_PREPREPARES_BACKUP = TMP_METRIC + 2009
    REPLICA_PREPREPARES_BACKUP = TMP_METRIC + 2010
    REPLICA_PREPARES_BACKUP = TMP_METRIC + 2011
    REPLICA_COMMITS_BACKUP = TMP_METRIC + 2012
    REPLICA_PRIMARYNAMES_BACKUP = TMP_METRIC + 2014
    REPLICA_STASHED_OUT_OF_ORDER_COMMITS_BACKUP = TMP_METRIC + 2015
    REPLICA_CHECKPOINTS_BACKUP = TMP_METRIC + 2016
    REPLICA_STASHED_RECVD_CHECKPOINTS_BACKUP = TMP_METRIC + 2017
    REPLICA_STASHING_WHILE_OUTSIDE_WATERMARKS_BACKUP = TMP_METRIC + 2018
    REPLICA_REQUEST_QUEUES_BACKUP = TMP_METRIC + 2019
    REPLICA_BATCHES_BACKUP = TMP_METRIC + 2020
    REPLICA_REQUESTED_PRE_PREPARES_BACKUP = TMP_METRIC + 2021
    REPLICA_REQUESTED_PREPARES_BACKUP = TMP_METRIC + 2022
    REPLICA_REQUESTED_COMMITS_BACKUP = TMP_METRIC + 2023
    REPLICA_PRE_PREPARES_STASHED_FOR_INCORRECT_TIME_BACKUP = TMP_METRIC + 2024

    REPLICA_ACTION_QUEUE_BACKUP = TMP_METRIC + 2025
    REPLICA_AQ_STASH_BACKUP = TMP_METRIC + 2026
    REPLICA_REPEATING_ACTIONS_BACKUP = TMP_METRIC + 2027
    REPLICA_SCHEDULED_BACKUP = TMP_METRIC + 2028

    # KV storages metrics
    STORAGE_IDR_CACHE_READERS = TMP_METRIC + 3001
    STORAGE_IDR_CACHE_TABLES_NUM = TMP_METRIC + 3002
    STORAGE_IDR_CACHE_TABLES_SIZE = TMP_METRIC + 3003

    STORAGE_ATTRIBUTE_STORE_READERS = TMP_METRIC + 3004
    STORAGE_ATTRIBUTE_STORE_TABLES_NUM = TMP_METRIC + 3005
    STORAGE_ATTRIBUTE_STORE_TABLES_SIZE = TMP_METRIC + 3006

    STORAGE_POOL_STATE_READERS = TMP_METRIC + 3007
    STORAGE_POOL_STATE_TABLES_NUM = TMP_METRIC + 3008
    STORAGE_POOL_STATE_TABLES_SIZE = TMP_METRIC + 3009

    STORAGE_DOMAIN_STATE_READERS = TMP_METRIC + 3010
    STORAGE_DOMAIN_STATE_TABLES_NUM = TMP_METRIC + 3011
    STORAGE_DOMAIN_STATE_TABLES_SIZE = TMP_METRIC + 3012

    STORAGE_CONFIG_STATE_READERS = TMP_METRIC + 3013
    STORAGE_CONFIG_STATE_TABLES_NUM = TMP_METRIC + 3014
    STORAGE_CONFIG_STATE_TABLES_SIZE = TMP_METRIC + 3015

    STORAGE_POOL_MANAGER_READERS = TMP_METRIC + 3016
    STORAGE_POOL_MANAGER_TABLES_NUM = TMP_METRIC + 3017
    STORAGE_POOL_MANAGER_TABLES_SIZE = TMP_METRIC + 3018

    STORAGE_BLS_BFT_READERS = TMP_METRIC + 3019
    STORAGE_BLS_BFT_TABLES_NUM = TMP_METRIC + 3020
    STORAGE_BLS_BFT_TABLES_SIZE = TMP_METRIC + 3021

    STORAGE_SEQ_NO_READERS = TMP_METRIC + 3022
    STORAGE_SEQ_NO_TABLES_NUM = TMP_METRIC + 3023
    STORAGE_SEQ_NO_TABLES_SIZE = TMP_METRIC + 3024

    STORAGE_METRICS_READERS = TMP_METRIC + 3025
    STORAGE_METRICS_TABLES_NUM = TMP_METRIC + 3026
    STORAGE_METRICS_TABLES_SIZE = TMP_METRIC + 3027


MetricsEvent = NamedTuple('MetricsEvent', [('timestamp', datetime), ('name', MetricsName),
                                           ('value', Union[float, ValueAccumulator])])


class MetricsCollector(ABC):
    @abstractmethod
    def store_event(self, name: MetricsName, value: Union[float, ValueAccumulator]):
        pass

    def __init__(self):
        self._accumulators = defaultdict(ValueAccumulator)

    def add_event(self, name: MetricsName, value: float):
        self._accumulators[name].add(value)

    def flush_accumulated(self):
        for name, value in self._accumulators.items():
            self.store_event(name, value)
        self._accumulators.clear()

    @contextmanager
    def measure_time(self, name: MetricsName):
        start = time.perf_counter()
        yield
        self.add_event(name, time.perf_counter() - start)


def measure_time(name: MetricsName, attr='metrics'):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            metrics = getattr(self, attr)
            with metrics.measure_time(name):
                return f(self, *args, **kwargs)

        return wrapper

    return decorator


def async_measure_time(name: MetricsName, attr='metrics'):
    def decorator(f):
        @functools.wraps(f)
        async def wrapper(self, *args, **kwargs):
            metrics = getattr(self, attr)
            with metrics.measure_time(name):
                return await f(self, *args, **kwargs)

        return wrapper

    return decorator


class NullMetricsCollector(MetricsCollector):
    def __init__(self):
        super().__init__()

    def store_event(self, name: MetricsName, value: Union[float, ValueAccumulator]):
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
        value = event.name.to_bytes(32, byteorder='big', signed=False)
        if isinstance(event.value, ValueAccumulator):
            value += event.value.to_bytes()
        else:
            value += struct.pack('d', event.value)
        return key, value

    @staticmethod
    def decode(key: bytes, value: bytes) -> Optional[MetricsEvent]:
        key = int.from_bytes(key, byteorder='big', signed=False)
        ts = datetime.utcfromtimestamp((key >> KvStoreMetricsFormat.seq_bits) / 1000000.0)
        name = int.from_bytes(value[:32], byteorder='big', signed=False)
        if name not in MetricsName.__members__.values():
            return None
        name = MetricsName(name)
        data = value[32:]
        if len(data) == 8:
            value = struct.unpack('d', data)[0]
        else:
            value = ValueAccumulator.from_bytes(data)
        return MetricsEvent(ts, name, value)


class KvStoreMetricsCollector(MetricsCollector):
    def __init__(self, storage: KeyValueStorage, ts_provider: Callable = datetime.utcnow):
        super().__init__()
        self._storage = storage
        self._ts_provider = ts_provider
        self._seq_no = 0

    def close(self):
        self._storage.close()

    def store_event(self, name: MetricsName, value: Union[float, ValueAccumulator]):
        if self._storage.closed:
            return

        event = MetricsEvent(self._ts_provider(), name, value)
        key, value = KvStoreMetricsFormat.encode(event, self._seq_no)
        self._storage.put(key, value)
        self._seq_no += 1
        self._seq_no = self._seq_no & KvStoreMetricsFormat.seq_mask

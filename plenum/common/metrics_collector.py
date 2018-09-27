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
    NODE_STACK_RX_MSGS = 30001
    CLIENT_STACK_RX_MSGS = 30002

    NODE_ACTION_QUEUE = 30003
    NODE_AQ_STASH = 30004
    NODE_REPEATING_ACTIONS = 30005
    NODE_SCHEDULED = 30006

    NODE_REQUESTED_PROPAGATES_FOR = 30007

    VIEW_CHANGER_ACTION_QUEUE = 30008
    VIEW_CHANGER_AQ_STASH = 30009
    VIEW_CHANGER_REPEATING_ACTIONS = 30010
    VIEW_CHANGER_SCHEDULED = 30011
    VIEW_CHANGER_INBOX = 30012
    VIEW_CHANGER_OUTBOX = 30013
    VIEW_CHANGER_NEXT_VIEW_INDICATIONS = 30014
    VIEW_CHANGER_VIEW_CHANGE_DONE = 30015

    PRIMARY_DECIDER_ACTION_QUEUE = 30016
    PRIMARY_DECIDER_AQ_STASH = 30017
    PRIMARY_DECIDER_REPEATING_ACTIONS = 30018
    PRIMARY_DECIDER_SCHEDULED = 30019
    PRIMARY_DECIDER_INBOX = 30020
    PRIMARY_DECIDER_OUTBOX = 30021

    MONITOR_NUM_ORDERED_REQUESTS = 30022
    MONITOR_THROUGHPUTS = 30023
    MONITOR_ORDERED_REQUESTS_IN_LAST = 30024

    MSGS_FOR_FUTURE_REPLICAS = 30025
    MSGS_TO_VIEW_CHANGER = 30026

    REQUEST_SENDER = 30027

    STASHED_ORDERED_REQS = 30028

    MSGS_FOR_FUTURE_VIEWS = 30029

    TXN_SEQ_RANGE_TO_3PHASE_KEY = 30030

    LAST_PERFORMANCE_CHECK_DATA = 30031

    LEDGERMANAGER_POOL_UNCOMMITEDS = 30032
    LEDGERMANAGER_DOMAIN_UNCOMMITEDS = 30033
    LEDGERMANAGER_CONFIG_UNCOMMITEDS = 30034

    # Collections metrics for master replica
    REPLICA_OUTBOX_MASTER = 31001
    REPLICA_INBOX_MASTER = 31002
    REPLICA_INBOX_STASH_MASTER = 31003
    REPLICA_POST_ELECTION_MSGS_MASTER = 31004
    REPLICA_PREPREPARES_PENDING_FIN_REQS_MASTER = 31005
    REPLICA_PREPREPARES_PENDING_PREVPP_MASTER = 31006
    REPLICA_PREPARES_WAITING_FOR_PREPREPARE_MASTER = 31007
    REPLICA_COMMITS_WAITING_FOR_PREPARE_MASTER = 31008
    REPLICA_SENT_PREPREPARES_MASTER = 31009
    REPLICA_PREPREPARES_MASTER = 31010
    REPLICA_PREPARES_MASTER = 31011
    REPLICA_COMMITS_MASTER = 31012
    REPLICA_ORDERED_MASTER = 31013
    REPLICA_PRIMARYNAMES_MASTER = 31014
    REPLICA_STASHED_OUT_OF_ORDER_COMMITS_MASTER = 31015
    REPLICA_CHECKPOINTS_MASTER = 31016
    REPLICA_STASHED_RECVD_CHECKPOINTS_MASTER = 31017
    REPLICA_STASHING_WHILE_OUTSIDE_WATERMARKS_MASTER = 31018
    REPLICA_REQUEST_QUEUES_MASTER = 31019
    REPLICA_BATCHES_MASTER = 31020
    REPLICA_REQUESTED_PRE_PREPARES_MASTER = 31021
    REPLICA_REQUESTED_PREPARES_MASTER = 31022
    REPLICA_REQUESTED_COMMITS_MASTER = 31023
    REPLICA_PRE_PREPARES_STASHED_FOR_INCORRECT_TIME_MASTER = 31024

    REPLICA_ACTION_QUEUE_MASTER = 31025
    REPLICA_AQ_STASH_MASTER = 31026
    REPLICA_REPEATING_ACTIONS_MASTER = 31027
    REPLICA_SCHEDULED_MASTER = 31028

    # Collections metrics for backup replica
    REPLICA_OUTBOX_BACKUP = 32001
    REPLICA_INBOX_BACKUP = 32002
    REPLICA_INBOX_STASH_BACKUP = 32003
    REPLICA_POST_ELECTION_MSGS_BACKUP = 32004
    REPLICA_PREPREPARES_PENDING_FIN_REQS_BACKUP = 32005
    REPLICA_PREPREPARES_PENDING_PREVPP_BACKUP = 32006
    REPLICA_PREPARES_WAITING_FOR_PREPREPARE_BACKUP = 32007
    REPLICA_COMMITS_WAITING_FOR_PREPARE_BACKUP = 32008
    REPLICA_SENT_PREPREPARES_BACKUP = 32009
    REPLICA_PREPREPARES_BACKUP = 32010
    REPLICA_PREPARES_BACKUP = 32011
    REPLICA_COMMITS_BACKUP = 32012
    REPLICA_ORDERED_BACKUP = 32013
    REPLICA_PRIMARYNAMES_BACKUP = 32014
    REPLICA_STASHED_OUT_OF_ORDER_COMMITS_BACKUP = 32015
    REPLICA_CHECKPOINTS_BACKUP = 32016
    REPLICA_STASHED_RECVD_CHECKPOINTS_BACKUP = 32017
    REPLICA_STASHING_WHILE_OUTSIDE_WATERMARKS_BACKUP = 32018
    REPLICA_REQUEST_QUEUES_BACKUP = 32019
    REPLICA_BATCHES_BACKUP = 32020
    REPLICA_REQUESTED_PRE_PREPARES_BACKUP = 32021
    REPLICA_REQUESTED_PREPARES_BACKUP = 32022
    REPLICA_REQUESTED_COMMITS_BACKUP = 32023
    REPLICA_PRE_PREPARES_STASHED_FOR_INCORRECT_TIME_BACKUP = 32024

    REPLICA_ACTION_QUEUE_BACKUP = 32025
    REPLICA_AQ_STASH_BACKUP = 32026
    REPLICA_REPEATING_ACTIONS_BACKUP = 32027
    REPLICA_SCHEDULED_BACKUP = 32028


MetricsEvent = NamedTuple('MetricsEvent', [('timestamp', datetime), ('name', MetricsName),
                                           ('value', Union[float, ValueAccumulator])])


class MetricsCollector(ABC):
    @abstractmethod
    def add_event(self, name: MetricsName, value: Union[float, ValueAccumulator]):
        pass

    def __init__(self):
        self._accumulators = defaultdict(ValueAccumulator)

    def acc_event(self, name: MetricsName, value: float):
        self._accumulators[name].add(value)

    def flush_accumulated(self):
        for name, value in self._accumulators.items():
            self.add_event(name, value)
        self._accumulators.clear()

    @contextmanager
    def measure_time(self, name: MetricsName):
        start = time.perf_counter()
        yield
        self.acc_event(name, time.perf_counter() - start)


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

    def add_event(self, name: MetricsName, value: Union[float, ValueAccumulator]):
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

    def add_event(self, name: MetricsName, value: Union[float, ValueAccumulator]):
        if self._storage.closed:
            return

        event = MetricsEvent(self._ts_provider(), name, value)
        key, value = KvStoreMetricsFormat.encode(event, self._seq_no)
        self._storage.put(key, value)
        self._seq_no += 1
        self._seq_no = self._seq_no & KvStoreMetricsFormat.seq_mask

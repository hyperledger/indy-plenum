from collections import namedtuple
from typing import NamedTuple, Any, List, Mapping, Optional, Dict, \
    Tuple

from stp_core.types import HA

NodeDetail = NamedTuple("NodeDetail", [
    ("ha", HA),
    ("cliname", str),
    ("cliha", HA)])

Field = namedtuple("Field", ["nm", "tp"])


class f:  # provides a namespace for reusable field constants
    REQUEST = Field('request', 'Request')
    REQUESTS = Field('requests', List[Any])
    MSG = Field('msg', str)
    NODE_NAME = Field('nodeName', str)
    NAME = Field("name", str)
    TIE_AMONG = Field("tieAmong", List[str])
    ROUND = Field("round", int)
    IDENTIFIER = Field('identifier', str)
    DIGEST = Field('digest', str)
    DIGESTS = Field('digests', List[str])
    RECEIVED_DIGESTS = Field('receivedDigests', Dict[str, str])
    SEQ_NO = Field('seqNo', int)
    SEQ_NO_START = Field('seqNoStart', int)
    SEQ_NO_END = Field('seqNoEnd', int)
    PP_SEQ_NO = Field('ppSeqNo', int)  # Pre-Prepare sequence number
    ORD_SEQ_NO = Field('ordSeqNo', int)     # Last PP_SEQ_NO that was ordered
    # Last ordered seq no of each protocol instance, sent during view change
    ORD_SEQ_NOS = Field('ordSeqNos', List[int])
    INSTANCES = Field('instancesIdr', List[int])
    RESULT = Field('result', Any)
    SENDER_NODE = Field('senderNode', str)
    REQ_ID = Field('reqId', int)
    VIEW_NO = Field('viewNo', int)
    LEDGER_INFO = Field("ledgerInfo", List[tuple])
    INST_ID = Field('instId', int)
    IS_STABLE = Field('isStable', bool)
    MSGS = Field('messages', List[Mapping])
    SIG = Field('signature', Optional[str])
    PROTOCOL_VERSION = Field('protocolVersion', int)
    SUSP_CODE = Field('suspicionCode', int)
    ELECTION_DATA = Field('electionData', Any)
    TXN_ID = Field('txnId', str)
    REASON = Field('reason', Any)
    IS_SUCCESS = Field('isSuccess', Any)
    SENDER_CLIENT = Field('senderClient', str)
    PP_TIME = Field("ppTime", float)
    REQ_IDR = Field("reqIdr", List[str])
    DISCARDED = Field("discarded", int)
    STATE_ROOT = Field("stateRootHash", str)
    POOL_STATE_ROOT_HASH = Field("poolStateRootHash", str)
    AUDIT_TXN_ROOT_HASH = Field("auditTxnRootHash", str)
    TXN_ROOT = Field("txnRootHash", str)
    BLS_SIG = Field("blsSig", str)
    BLS_MULTI_SIG = Field("blsMultiSig", str)
    BLS_MULTI_SIG_STATE_ROOT = Field("blsMultiSigStateRoot", str)
    MERKLE_ROOT = Field("merkleRoot", str)
    OLD_MERKLE_ROOT = Field("oldMerkleRoot", str)
    NEW_MERKLE_ROOT = Field("newMerkleRoot", str)
    TXN_SEQ_NO = Field("txnSeqNo", int)
    # 0 for pool transaction ledger, 1 for domain transaction ledger
    LEDGER_ID = Field("ledgerId", int)
    CATCHUP_TILL = Field("catchupTill", int)
    HASHES = Field("hashes", List[str])
    TXNS = Field("txns", List[Any])
    TXN = Field("txn", Any)
    NODES = Field('nodes', Dict[str, HA])
    CONS_PROOF = Field("consProof", Any)
    MSG_TYPE = Field("msg_type", str)
    PARAMS = Field("params", dict)
    PRIMARY = Field("primary", dict)
    SIGS = Field('signatures', dict)
    PLUGIN_FIELDS = Field('plugin_fields', dict)
    FEES = Field('fees', dict)
    SUB_SEQ_NO = Field('sub_seq_no', int)
    FINAL = Field('final', bool)
    VALID_REQ_IDR = Field("valid_reqIdr", List[str])
    INVALID_REQ_IDR = Field("invalid_reqIdr", List[str])


OPERATION = 'operation'


PLUGIN_TYPE_VERIFICATION = "VERIFICATION"
PLUGIN_TYPE_PROCESSING = "PROCESSING"
PLUGIN_TYPE_STATS_CONSUMER = "STATS_CONSUMER"
PLUGIN_TYPE_AUTHENTICATOR = 'AUTHENTICATOR'

EVENT_REQ_ORDERED = "req_ordered"
EVENT_NODE_STARTED = "node_started"
EVENT_PERIODIC_STATS_THROUGHPUT = "periodic_stats_throughput"
EVENT_VIEW_CHANGE = "view_changed"
EVENT_PERIODIC_STATS_LATENCIES = "periodic_stats_latencies"
EVENT_PERIODIC_STATS_NODES = "periodic_stats_nodes"
EVENT_PERIODIC_STATS_NODE_INFO = "periodic_stats_node_info"
EVENT_PERIODIC_STATS_SYSTEM_PERFORMANCE_INFO = "periodic_stats_system_performance_info"
EVENT_PERIODIC_STATS_TOTAL_REQUESTS = "periodic_stats_total_requests"

import sys
from collections import namedtuple
from hashlib import sha256
from typing import NamedTuple, Any, List, Mapping, Optional, TypeVar, Dict

from plenum.common.txn import NOMINATE, PRIMARY, REELECTION, REQDIGEST, REQACK,\
    ORDERED, PROPAGATE, PREPREPARE, REPLY, COMMIT, PREPARE, BATCH, INSTANCE_CHANGE, \
    BLACKLIST, REQNACK, LEDGER_STATUS, LEDGER_STATUSES, CONSISTENCY_PROOFS, \
    CONSISTENCY_PROOF, CATCHUP_REQ, CATCHUP_REP, CATCHUP_REQS, CATCHUP_REPS, \
    CLINODEREG

HA = NamedTuple("HA", [
    ("host", str),
    ("port", int)])

NodeDetail = NamedTuple("NodeDetail", [
    ("ha", HA),
    ("cliname", str),
    ("cliha", HA)])


Field = namedtuple("Field", ["nm", "tp"])


class f:  # provides a namespace for reusable field constants
    REQUEST = Field('request', 'Request')
    MSG = Field('msg', str)
    NODE_NAME = Field('nodeName', str)
    NAME = Field("name", str)
    TIE_AMONG = Field("tieAmong", List[str])
    ROUND = Field("round", int)
    IDENTIFIER = Field('identifier', str)
    DIGEST = Field('digest', str)
    PP_SEQ_NO = Field('ppSeqNo', int)  # Pre-Prepare sequence number
    RESULT = Field('result', Any)
    SENDER_NODE = Field('senderNode', str)
    REQ_ID = Field('reqId', int)
    VIEW_NO = Field('viewNo', int)
    INST_ID = Field('instId', int)
    MSGS = Field('messages', List[Mapping])
    SIG = Field('signature', Optional[str])
    SUSP_CODE = Field('suspicionCode', int)
    ELECTION_DATA = Field('electionData', Any)
    TXN_ID = Field('txnId', str)
    REASON = Field('reason', Any)
    SENDER_CLIENT = Field('senderClient', str)
    PP_TIME = Field("ppTime", float)
    MERKLE_ROOT = Field("merkleRoot", str)
    OLD_MERKLE_ROOT = Field("oldMerkleRoot", str)
    NEW_MERKLE_ROOT = Field("newMerkleRoot", str)
    TXN_SEQ_NO = Field("txnSeqNo", int)
    # 0 for pool transaction ledger, 1 for domain transaction ledger
    LEDGER_TYPE = Field("ledgerType", int)
    SEQ_NO_START = Field("seqNoStart", int)
    SEQ_NO_END = Field("seqNoEnd", int)
    HASHES = Field("hashes", List[str])
    TXNS = Field("txns", List[Any])
    NODES = Field('nodes', Dict[str, HA])
    # POOL_LEDGER_STATUS = Field("poolLedgerStatus", LedgerStatus)
    # DOMAIN_LEDGER_STATUS = Field("domainLedgerStatus", LedgerStatus)
    # CONS_PROOF = Field("consProof", ConsistencyProof)
    # POOL_CONS_PROOF = Field("poolConsProof", ConsistencyProof)
    # DOMAIN_CONS_PROOF = Field("domainConsProof", ConsistencyProof)
    # POOL_CATCHUP_REQ = Field("poolCatchupReq", CatchupReq)
    # DOMAIN_CATCHUP_REQ = Field("domainCatchupReq", CatchupReq)
    # POOL_CATCHUP_REP = Field("poolCatchupRep", CatchupRep)
    # DOMAIN_CATCHUP_REP = Field("domainCatchupRep", CatchupRep)
    POOL_LEDGER_STATUS = Field("poolLedgerStatus", Any)
    DOMAIN_LEDGER_STATUS = Field("domainLedgerStatus", Any)
    CONS_PROOF = Field("consProof", Any)
    POOL_CONS_PROOF = Field("poolConsProof", Any)
    DOMAIN_CONS_PROOF = Field("domainConsProof", Any)
    POOL_CATCHUP_REQ = Field("poolCatchupReq", Any)
    DOMAIN_CATCHUP_REQ = Field("domainCatchupReq", Any)
    POOL_CATCHUP_REP = Field("poolCatchupRep", Any)
    DOMAIN_CATCHUP_REP = Field("domainCatchupRep", Any)


# TODO: Move this to `txn.py` which should be renamed to constants.py
OP_FIELD_NAME = "op"


class TaggedTupleBase:
    def melted(self):
        """
        Return the tagged tuple in a dictionary form.
        """
        if hasattr(self, "__dict__"):
            m = self.__dict__
        elif hasattr(self, "_asdict"):
            m = self._asdict()
        else:
            raise RuntimeError("Cannot convert argument to a dictionary")
        m[OP_FIELD_NAME] = self.typename
        m.move_to_end(OP_FIELD_NAME, False)
        return m


# noinspection PyProtectedMember
def TaggedTuple(typename, fields):
    cls = NamedTuple(typename, fields)
    if any(field == OP_FIELD_NAME for field in cls._fields):
            raise RuntimeError("field name '{}' is reserved in TaggedTuple"
                               .format(OP_FIELD_NAME))
    cls.__bases__ += (TaggedTupleBase,)
    cls.typename = typename
    return cls

Nomination = TaggedTuple(NOMINATE, [
    f.NAME,
    f.INST_ID,
    f.VIEW_NO])

Batch = TaggedTuple(BATCH, [
    f.MSGS,
    f.SIG])

# Reelection messages that nodes send when they find the 2 or more nodes have
# equal nominations for primary. `round` indicates the reelection round
# number. So the first reelection would have round number 1, the one after
# that would have round number 2. If a node receives a reelection message with
# a round number that is not 1 greater than the reelections rounds it has
# already seen then it rejects that message
Reelection = TaggedTuple(REELECTION, [
    f.INST_ID,
    f.ROUND,
    f.TIE_AMONG,
    f.VIEW_NO])

# Declaration of a winner
Primary = TaggedTuple(PRIMARY, [
    f.NAME,
    f.INST_ID,
    f.VIEW_NO])

BlacklistMsg = NamedTuple(BLACKLIST, [
    f.SUSP_CODE,
    f.NODE_NAME])


OPERATION = 'operation'


class Request:
    def __init__(self,
                 identifier: str=None,
                 reqId: int=None,
                 operation: Mapping=None,
                 signature: str=None):
        self.identifier = identifier
        self.reqId = reqId
        self.operation = operation
        self.signature = signature

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "{}: {}".format(self.__class__.__name__, self.__dict__)

    @property
    def key(self):
        return self.identifier, self.reqId

    @property
    def digest(self):
        return sha256("{}{}".format(*self.key).encode('utf-8')).hexdigest()

    @property
    def reqDigest(self):
        return ReqDigest(self.identifier, self.reqId, self.digest)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)
        return self

    @classmethod
    def fromState(cls, state):
        obj = cls.__new__(cls)
        cls.__setstate__(obj, state)
        return obj


class ReqDigest(NamedTuple(REQDIGEST, [f.IDENTIFIER,
                                       f.REQ_ID,
                                       f.DIGEST])):
    def key(self):
        return self.identifier, self.reqId


RequestAck = TaggedTuple(REQACK, [
    f.REQ_ID])

RequestNack = TaggedTuple(REQNACK, [
    f.REQ_ID,
    f.REASON])

NodeRegForClient = TaggedTuple(CLINODEREG, [
    f.NODES])

Ordered = NamedTuple(ORDERED, [
    f.INST_ID,
    f.VIEW_NO,
    f.IDENTIFIER,
    f.REQ_ID,
    f.DIGEST,
    f.PP_TIME])

# <PROPAGATE, <REQUEST, o, s, c> σc, i>~μi
# s = client sequence number (comes from Aardvark paper)

Propagate = TaggedTuple(PROPAGATE, [
    f.REQUEST,
    f.SENDER_CLIENT])

PrePrepare = TaggedTuple(PREPREPARE, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO,
    f.IDENTIFIER,
    f.REQ_ID,
    f.DIGEST,
    f.PP_TIME
    ])

Prepare = TaggedTuple(PREPARE, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO,
    f.DIGEST,
    f.PP_TIME])

Commit = TaggedTuple(COMMIT, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO,
    f.DIGEST,
    f.PP_TIME])

# TODO Refactor this. Reply should simply a wrapper over a dict, or just a dict?
Reply = TaggedTuple(REPLY, [f.RESULT])

InstanceChange = TaggedTuple(INSTANCE_CHANGE, [
    f.VIEW_NO
])

LedgerStatus = TaggedTuple(LEDGER_STATUS, [
    f.LEDGER_TYPE,
    f.TXN_SEQ_NO,
    f.MERKLE_ROOT])

LedgerStatuses = TaggedTuple(LEDGER_STATUSES, [
    f.POOL_LEDGER_STATUS,
    f.DOMAIN_LEDGER_STATUS
])

ConsistencyProof = TaggedTuple(CONSISTENCY_PROOF, [
    f.LEDGER_TYPE,
    f.SEQ_NO_START,
    f.SEQ_NO_END,
    f.OLD_MERKLE_ROOT,
    f.NEW_MERKLE_ROOT,
    f.HASHES
])

ConsistencyProofs = TaggedTuple(CONSISTENCY_PROOFS, [
    f.POOL_CONS_PROOF,
    f.DOMAIN_CONS_PROOF
])


# TODO: Catchup is not a good name, replace it with `sync` or something which
# is familiar

CatchupReq = TaggedTuple(CATCHUP_REQ, [
    f.LEDGER_TYPE,
    f.SEQ_NO_START,
    f.SEQ_NO_END,
])

CatchupReqs = TaggedTuple(CATCHUP_REQS, [
    f.POOL_CATCHUP_REQ,
    f.DOMAIN_CATCHUP_REQ
])

CatchupRep = TaggedTuple(CATCHUP_REP, [
    f.LEDGER_TYPE,
    f.TXNS,
    f.CONS_PROOF
])

CatchupReps = TaggedTuple(CATCHUP_REPS, [
    f.POOL_CATCHUP_REP,
    f.DOMAIN_CATCHUP_REP
])

TaggedTuples = None  # type: Dict[str, class]


def loadRegistry():
    global TaggedTuples
    if not TaggedTuples:
        this = sys.modules[__name__]
        TaggedTuples = {getattr(this, x).__name__: getattr(this, x)
                        for x in dir(this) if
                        callable(getattr(getattr(this, x), "melted", None))
                        and getattr(getattr(this, x), "_fields", None)}

loadRegistry()

ThreePhaseMsg = TypeVar("3PhaseMsg",
                        PrePrepare,
                        Prepare,
                        Commit)

ThreePhaseKey = NamedTuple("ThreePhaseKey", [
                        f.VIEW_NO,
                        f.PP_SEQ_NO
                    ])

CLIENT_STACK_SUFFIX = "C"
CLIENT_BLACKLISTER_SUFFIX = "BLC"
NODE_BLACKLISTER_SUFFIX = "BLN"

NODE_PRIMARY_STORAGE_SUFFIX = "PS"
NODE_SECONDARY_STORAGE_SUFFIX = "SS"
NODE_TXN_STORE_SUFFIX = "TS"
NODE_HASH_STORE_SUFFIX = "HS"

HS_FILE = "file"
HS_ORIENT_DB = "orientdb"
HS_MEMORY = "memory"

PLUGIN_TYPE_VERIFICATION = "VERIFICATION"
PLUGIN_TYPE_PROCESSING = "PROCESSING"
PLUGIN_TYPE_STATS_CONSUMER = "STATS_CONSUMER"

EVENT_REQ_ORDERED = "req_ordered"
EVENT_NODE_STARTED = "node_started"
EVENT_PERIODIC_STATS_THROUGHPUT = "periodic_stats_throughput"
EVENT_VIEW_CHANGE = "view_changed"
EVENT_PERIODIC_STATS_LATENCIES = "periodic_stats_latencies"
PLUGIN_BASE_DIR_PATH = "PluginBaseDirPath"

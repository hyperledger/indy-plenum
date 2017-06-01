from typing import NamedTuple, Any, List, Mapping, Optional, TypeVar, Dict, \
    Tuple

import sys
from collections import namedtuple

from plenum.common.constants import NOMINATE, PRIMARY, REELECTION, REQACK, \
    ORDERED, PROPAGATE, PREPREPARE, REPLY, COMMIT, PREPARE, BATCH, \
    INSTANCE_CHANGE, BLACKLIST, REQNACK, LEDGER_STATUS, CONSISTENCY_PROOF, \
    CATCHUP_REQ, CATCHUP_REP, POOL_LEDGER_TXNS, CONS_PROOF_REQUEST, CHECKPOINT, \
    CHECKPOINT_STATE, THREE_PC_STATE, REJECT, OP_FIELD_NAME, POOL_LEDGER_ID, DOMAIN_LEDGER_ID
from plenum.common.messages.client_request import ClientOperationField
from plenum.common.messages.fields import *
from plenum.common.messages.fields import IdentifierField, NonNegativeNumberField, SignatureField
from plenum.common.messages.message_base import MessageBase, MessageValidator
from stp_core.types import HA

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
    DIGESTS = Field('digests', List[str])
    RECEIVED_DIGESTS = Field('receivedDigests', Dict[str, str])
    SEQ_NO = Field('seqNo', int)
    PP_SEQ_NO = Field('ppSeqNo', int)  # Pre-Prepare sequence number
    ORD_SEQ_NO = Field('ordSeqNo', int)     # Last PP_SEQ_NO that was ordered
    ORD_SEQ_NOS = Field('ordSeqNos', List[int])  # Last ordered seq no of each protocol instance, sent during view change
    RESULT = Field('result', Any)
    SENDER_NODE = Field('senderNode', str)
    REQ_ID = Field('reqId', int)
    VIEW_NO = Field('viewNo', int)
    INST_ID = Field('instId', int)
    IS_STABLE = Field('isStable', bool)
    MSGS = Field('messages', List[Mapping])
    SIG = Field('signature', Optional[str])
    SUSP_CODE = Field('suspicionCode', int)
    ELECTION_DATA = Field('electionData', Any)
    TXN_ID = Field('txnId', str)
    REASON = Field('reason', Any)
    SENDER_CLIENT = Field('senderClient', str)
    PP_TIME = Field("ppTime", float)
    REQ_IDR = Field("reqIdr", List[Tuple[str, int]])
    DISCARDED = Field("discarded", int)
    STATE_ROOT = Field("stateRootHash", str)
    TXN_ROOT = Field("txnRootHash", str)
    MERKLE_ROOT = Field("merkleRoot", str)
    OLD_MERKLE_ROOT = Field("oldMerkleRoot", str)
    NEW_MERKLE_ROOT = Field("newMerkleRoot", str)
    TXN_SEQ_NO = Field("txnSeqNo", int)
    # 0 for pool transaction ledger, 1 for domain transaction ledger
    LEDGER_ID = Field("ledgerId", int)
    SEQ_NO_START = Field("seqNoStart", int)
    SEQ_NO_END = Field("seqNoEnd", int)
    CATCHUP_TILL = Field("catchupTill", int)
    HASHES = Field("hashes", List[str])
    TXNS = Field("txns", List[Any])
    TXN = Field("txn", Any)
    NODES = Field('nodes', Dict[str, HA])
    POOL_LEDGER_STATUS = Field("poolLedgerStatus", Any)
    DOMAIN_LEDGER_STATUS = Field("domainLedgerStatus", Any)
    CONS_PROOF = Field("consProof", Any)
    POOL_CONS_PROOF = Field("poolConsProof", Any)
    DOMAIN_CONS_PROOF = Field("domainConsProof", Any)
    POOL_CATCHUP_REQ = Field("poolCatchupReq", Any)
    DOMAIN_CATCHUP_REQ = Field("domainCatchupReq", Any)
    POOL_CATCHUP_REP = Field("poolCatchupRep", Any)
    DOMAIN_CATCHUP_REP = Field("domainCatchupRep", Any)


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
def TaggedTuple(typename, fields) -> NamedTuple:
    cls = NamedTuple(typename, fields)
    if OP_FIELD_NAME in cls._fields:
        raise RuntimeError("field name '{}' is reserved in TaggedTuple"
                           .format(OP_FIELD_NAME))
    cls.__bases__ += (TaggedTupleBase,)
    cls.typename = typename
    return cls

OPERATION = 'operation'


class ClientMessageValidator(MessageValidator):
    schema = (
        (f.IDENTIFIER.nm, IdentifierField()),
        (f.REQ_ID.nm, NonNegativeNumberField()),
        (OPERATION, ClientOperationField()),
        (f.SIG.nm, SignatureField(optional=True)),
        (f.DIGEST.nm, NonEmptyStringField(optional=True)),
    )


# class Nomination(MessageBase):
#     typename = NOMINATE
#
#     schema = (
#         (f.NAME.nm, NonEmptyStringField()),
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#         (f.ORD_SEQ_NO.nm, NonNegativeNumberField()),
#     )
Nomination = TaggedTuple(NOMINATE, [
    f.NAME,
    f.INST_ID,
    f.VIEW_NO,
    f.ORD_SEQ_NO])


Batch = TaggedTuple(BATCH, [
    f.MSGS,
    f.SIG])

# Reelection messages that nodes send when they find the 2 or more nodes have
# equal nominations for primary. `round` indicates the reelection round
# number. So the first reelection would have round number 1, the one after
# that would have round number 2. If a node receives a reelection message with
# a round number that is not 1 greater than the reelections rounds it has
# already seen then it rejects that message


# class Reelection(MessageBase):
#     typename = REELECTION
#
#     schema = (
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.ROUND.nm, NonNegativeNumberField()),
#         (f.TIE_AMONG.nm, IterableField(TieAmongField())),
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#     )
Reelection = TaggedTuple(REELECTION, [
    f.INST_ID,
    f.ROUND,
    f.TIE_AMONG,
    f.VIEW_NO])

# Declaration of a winner

# class Primary(MessageBase):
#     typename = PRIMARY
#
#     schema = (
#         (f.NAME.nm, NonEmptyStringField()),
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#         (f.ORD_SEQ_NO.nm, NonNegativeNumberField()),
#     )
Primary = TaggedTuple(PRIMARY, [
    f.NAME,
    f.INST_ID,
    f.VIEW_NO,
    f.ORD_SEQ_NO])

BlacklistMsg = NamedTuple(BLACKLIST, [
    f.SUSP_CODE,
    f.NODE_NAME])

RequestAck = TaggedTuple(REQACK, [
    f.IDENTIFIER,
    f.REQ_ID])

RequestNack = TaggedTuple(REQNACK, [
    f.IDENTIFIER,
    f.REQ_ID,
    f.REASON])

Reject = TaggedTuple(REJECT, [
    f.IDENTIFIER,
    f.REQ_ID,
    f.REASON])

PoolLedgerTxns = TaggedTuple(POOL_LEDGER_TXNS, [
    f.TXN
])


# class Ordered(MessageBase):
#     typename = ORDERED
#     schema = (
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#         (f.REQ_IDR.nm, IterableField(RequestIdentifierField())),
#         (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
#         (f.PP_TIME.nm, TimestampField()),
#         (f.LEDGER_ID.nm, LedgerIdField()),
#         (f.STATE_ROOT.nm, MerkleRootField()),
#         (f.TXN_ROOT.nm, MerkleRootField()),
#     )
Ordered = NamedTuple(ORDERED, [
    f.INST_ID,
    f.VIEW_NO,
    f.REQ_IDR,
    f.PP_SEQ_NO,
    f.PP_TIME,
    f.LEDGER_ID,
    f.STATE_ROOT,
    f.TXN_ROOT,
    ])

# <PROPAGATE, <REQUEST, o, s, c> σc, i>~μi
# s = client sequence number (comes from Aardvark paper)

# Propagate needs the name of the sender client since every node needs to know
# who sent the request to send the reply. If all clients had name same as
# their identifier same as client name (stack name, the name which RAET knows)


# class Propagate(MessageBase):
#     typename = PROPAGATE
#     schema = (
#         (f.REQUEST.nm, ClientMessageValidator()),
#         (f.SENDER_CLIENT.nm, NonEmptyStringField()),
#     )
Propagate = TaggedTuple(PROPAGATE, [
    f.REQUEST,
    f.SENDER_CLIENT])


# class PrePrepare(MessageBase):
#     typename = PREPREPARE
#     schema = (
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#         (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
#         (f.PP_TIME.nm, TimestampField()),
#         (f.REQ_IDR.nm, IterableField(RequestIdentifierField())),
#         (f.DISCARDED.nm, NonNegativeNumberField()),
#         (f.DIGEST.nm, NonEmptyStringField()),
#         (f.LEDGER_ID.nm, LedgerIdField()),
#         (f.STATE_ROOT.nm, MerkleRootField()),
#         (f.TXN_ROOT.nm, MerkleRootField()),
#     )
PrePrepare = TaggedTuple(PREPREPARE, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO,
    f.PP_TIME,
    f.REQ_IDR,
    f.DISCARDED,
    f.DIGEST,
    f.LEDGER_ID,
    f.STATE_ROOT,
    f.TXN_ROOT,
    ])


# class Prepare(MessageBase):
#     typename = PREPARE
#     schema = (
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#         (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
#         (f.DIGEST.nm, NonEmptyStringField()),
#         (f.STATE_ROOT.nm, HexField(length=64, nullable=True)),
#         (f.TXN_ROOT.nm, HexField(length=64, nullable=True)),
#     )
Prepare = TaggedTuple(PREPARE, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO,
    f.DIGEST,
    f.STATE_ROOT,
    f.TXN_ROOT,
    ])


# class Commit(MessageBase):
#     typename = COMMIT
#     schema = (
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#         (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
#     )
Commit = TaggedTuple(COMMIT, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO
    ])

# class Checkpoint(MessageBase):
#     typename = CHECKPOINT
#     schema = (
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#         (f.SEQ_NO_START.nm, NonNegativeNumberField()),
#         (f.SEQ_NO_END.nm, NonNegativeNumberField()),
#         (f.DIGEST.nm, NonEmptyStringField()),
#     )
Checkpoint = TaggedTuple(CHECKPOINT, [
    f.INST_ID,
    f.VIEW_NO,
    f.SEQ_NO_START,
    f.SEQ_NO_END,
    f.DIGEST])


CheckpointState = NamedTuple(CHECKPOINT_STATE, [
    f.SEQ_NO,   # Current ppSeqNo in the checkpoint
    f.DIGESTS,  # Digest of all the requests in the checkpoint
    f.DIGEST,   # Final digest of the checkpoint, after all requests in its
    # range have been ordered
    f.RECEIVED_DIGESTS,
    f.IS_STABLE
    ])


# class ThreePCState(MessageBase):
#     typename = THREE_PC_STATE
#     schema = (
#         (f.INST_ID.nm, NonNegativeNumberField()),
#         (f.MSGS.nm, IterableField(ClientMessageValidator())),
#     )
ThreePCState = TaggedTuple(THREE_PC_STATE, [
    f.INST_ID,
    f.MSGS])

Reply = TaggedTuple(REPLY, [f.RESULT])

# class InstanceChange(MessageBase):
#     typename = INSTANCE_CHANGE
#     schema = (
#         (f.VIEW_NO.nm, NonNegativeNumberField()),
#         (f.REASON.nm, NonNegativeNumberField()),
#         (f.ORD_SEQ_NOS.nm, IterableField(NonNegativeNumberField())),
#     )
InstanceChange = TaggedTuple(INSTANCE_CHANGE, [
    f.VIEW_NO,
    f.REASON,
    f.ORD_SEQ_NOS,
])


# class LedgerStatus(MessageBase):
#     typename = LEDGER_STATUS
#     schema = (
#         (f.LEDGER_ID.nm, LedgerIdField()),
#         (f.TXN_SEQ_NO.nm, NonNegativeNumberField()),
#         (f.MERKLE_ROOT.nm, MerkleRootField()),
#     )
LedgerStatus = TaggedTuple(LEDGER_STATUS, [
    f.LEDGER_ID,
    f.TXN_SEQ_NO,
    f.MERKLE_ROOT])


# class ConsistencyProof(MessageBase):
#     typename = CONSISTENCY_PROOF
#     schema = (
#         (f.LEDGER_ID.nm, LedgerIdField()),
#         (f.SEQ_NO_START.nm, NonNegativeNumberField()),
#         (f.SEQ_NO_END.nm, NonNegativeNumberField()),
#         (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
#         (f.OLD_MERKLE_ROOT.nm, MerkleRootField()),
#         (f.NEW_MERKLE_ROOT.nm, MerkleRootField()),
#         (f.HASHES.nm, IterableField(NonEmptyStringField())),
#     )
ConsistencyProof = TaggedTuple(CONSISTENCY_PROOF, [
    f.LEDGER_ID,
    f.SEQ_NO_START,
    f.SEQ_NO_END,
    f.PP_SEQ_NO,
    f.OLD_MERKLE_ROOT,
    f.NEW_MERKLE_ROOT,
    f.HASHES
])

# TODO: Catchup is not a good name, replace it with `sync` or something which
# is familiar

# class CatchupReq(MessageBase):
#     typename = CATCHUP_REQ
#     schema = (
#         (f.LEDGER_ID.nm, LedgerIdField()),
#         (f.SEQ_NO_START.nm, NonNegativeNumberField()),
#         (f.SEQ_NO_END.nm, NonNegativeNumberField()),
#         (f.CATCHUP_TILL.nm, NonNegativeNumberField()),
#     )
CatchupReq = TaggedTuple(CATCHUP_REQ, [
    f.LEDGER_ID,
    f.SEQ_NO_START,
    f.SEQ_NO_END,
    f.CATCHUP_TILL
])


# class CatchupRep(MessageBase):
#     typename = CATCHUP_REQ
#     schema = (
#         (f.LEDGER_ID.nm, LedgerIdField()),
#         (f.TXNS.nm, IterableField(ClientMessageValidator())),
#         (f.CONS_PROOF.nm, IterableField(HexField(length=64))),
#     )
CatchupRep = TaggedTuple(CATCHUP_REP, [
    f.LEDGER_ID,
    f.TXNS,
    f.CONS_PROOF
])

# class ConsProofRequest(MessageBase):
#     typename = CONS_PROOF_REQUEST
#     schema = (
#         (f.LEDGER_ID.nm, LedgerIdField()),
#         (f.SEQ_NO_START.nm, NonNegativeNumberField()),
#         (f.SEQ_NO_END.nm, NonNegativeNumberField()),
#     )
ConsProofRequest = TaggedTuple(CONS_PROOF_REQUEST, [
    f.LEDGER_ID,
    f.SEQ_NO_START,
    f.SEQ_NO_END
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
        # attach MessageBase, for pre-testing procedure
        # TODO: add MessageBase classes another way
        TaggedTuples.update(
            {getattr(this, x).typename: getattr(this, x)
             for x in dir(this)
             if getattr(getattr(this, x), "schema", None) and issubclass(getattr(this, x), MessageBase)}
        )

loadRegistry()

ThreePhaseType = (PrePrepare, Prepare, Commit)
ThreePhaseMsg = TypeVar("3PhaseMsg", *ThreePhaseType)


ElectionType = (Nomination, Primary, Reelection)
ElectionMsg = TypeVar("ElectionMsg", *ElectionType)

ThreePhaseKey = NamedTuple("ThreePhaseKey", [
                        f.VIEW_NO,
                        f.PP_SEQ_NO
                    ])

PLUGIN_TYPE_VERIFICATION = "VERIFICATION"
PLUGIN_TYPE_PROCESSING = "PROCESSING"
PLUGIN_TYPE_STATS_CONSUMER = "STATS_CONSUMER"

EVENT_REQ_ORDERED = "req_ordered"
EVENT_NODE_STARTED = "node_started"
EVENT_PERIODIC_STATS_THROUGHPUT = "periodic_stats_throughput"
EVENT_VIEW_CHANGE = "view_changed"
EVENT_PERIODIC_STATS_LATENCIES = "periodic_stats_latencies"
EVENT_PERIODIC_STATS_NODES = "periodic_stats_nodes"
EVENT_PERIODIC_STATS_NODE_INFO = "periodic_stats_node_info"
EVENT_PERIODIC_STATS_SYSTEM_PERFORMANCE_INFO = "periodic_stats_system_performance_info"
EVENT_PERIODIC_STATS_TOTAL_REQUESTS = "periodic_stats_total_requests"

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


OPERATION = 'operation'


class ClientMessageValidator(MessageValidator):

    def __init__(self, operation_schema_is_strict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Following code is for support of non-strict schema
        # TODO: refactor this
        # TODO: this (and all related functionality) can be removed when
        # when fixed problem with transaction serialization (INDY-338)
        strict = operation_schema_is_strict
        if not strict:
            operation_field_index = 2
            op = ClientOperationField(schema_is_strict=False)
            schema = list(self.schema)
            schema[operation_field_index] = (OPERATION, op)
            self.schema = tuple(schema)

    schema = (
        (f.IDENTIFIER.nm, IdentifierField()),
        (f.REQ_ID.nm, NonNegativeNumberField()),
        (OPERATION, ClientOperationField()),
        (f.SIG.nm, SignatureField(optional=True)),
        (f.DIGEST.nm, NonEmptyStringField(optional=True)),
    )


class Nomination(MessageBase):
    typename = NOMINATE

    schema = (
        (f.NAME.nm, NonEmptyStringField()),
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.ORD_SEQ_NO.nm, NonNegativeNumberField()),
    )


class Batch(MessageBase):
    typename = BATCH

    schema = (
        (f.MSGS.nm, IterableField(SerializedValueField())),
        (f.SIG.nm, SignatureField()),
    )


# Reelection messages that nodes send when they find the 2 or more nodes have
# equal nominations for primary. `round` indicates the reelection round
# number. So the first reelection would have round number 1, the one after
# that would have round number 2. If a node receives a reelection message with
# a round number that is not 1 greater than the reelections rounds it has
# already seen then it rejects that message


class Reelection(MessageBase):
    typename = REELECTION

    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.ROUND.nm, NonNegativeNumberField()),
        (f.TIE_AMONG.nm, IterableField(TieAmongField())),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
    )

class Primary(MessageBase):
    typename = PRIMARY

    schema = (
        (f.NAME.nm, NonEmptyStringField()),
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.ORD_SEQ_NO.nm, NonNegativeNumberField()),
    )

# TODO implement actual rules
class BlacklistMsg(MessageBase):
    typename = BLACKLIST
    schema = (
        (f.SUSP_CODE.nm, AnyValueField()),
        (f.NODE_NAME.nm, AnyValueField()),
    )

# TODO implement actual rules
class RequestAck(MessageBase):
    typename = REQACK
    schema = (
        (f.IDENTIFIER.nm, AnyValueField()),
        (f.REQ_ID.nm, AnyValueField())
    )

# TODO implement actual rules
class RequestNack(MessageBase):
    typename = REQNACK
    schema = (
        (f.IDENTIFIER.nm, AnyValueField()),
        (f.REQ_ID.nm, AnyValueField()),
        (f.REASON.nm, AnyValueField()),
    )

# TODO implement actual rules
class Reject(MessageBase):
    typename = REJECT
    schema = (
        (f.IDENTIFIER.nm, AnyValueField()),
        (f.REQ_ID.nm, AnyValueField()),
        (f.REASON.nm, AnyValueField()),
    )

# TODO implement actual rules
class PoolLedgerTxns(MessageBase):
    typename = POOL_LEDGER_TXNS
    schema = (
        (f.TXN.nm, AnyValueField()),
    )


class Ordered(MessageBase):
    typename = ORDERED
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.REQ_IDR.nm, IterableField(RequestIdentifierField())),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.PP_TIME.nm, TimestampField()),
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.STATE_ROOT.nm, HexField(length=64, nullable=True)),
        (f.TXN_ROOT.nm, HexField(length=64, nullable=True)),
    )

# <PROPAGATE, <REQUEST, o, s, c> σc, i>~μi
# s = client sequence number (comes from Aardvark paper)

class Propagate(MessageBase):
    typename = PROPAGATE
    schema = (
        (f.REQUEST.nm, ClientMessageValidator(operation_schema_is_strict=True)),
        (f.SENDER_CLIENT.nm, NonEmptyStringField()),
    )


class PrePrepare(MessageBase):
    typename = PREPREPARE
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.PP_TIME.nm, TimestampField()),
        (f.REQ_IDR.nm, IterableField(RequestIdentifierField())),
        (f.DISCARDED.nm, NonNegativeNumberField()),
        (f.DIGEST.nm, NonEmptyStringField()),
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.STATE_ROOT.nm, HexField(length=64, nullable=True)),
        (f.TXN_ROOT.nm, HexField(length=64, nullable=True)),
    )


class Prepare(MessageBase):
    typename = PREPARE
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.DIGEST.nm, NonEmptyStringField()),
        (f.STATE_ROOT.nm, HexField(length=64, nullable=True)),
        (f.TXN_ROOT.nm, HexField(length=64, nullable=True)),
    )


class Commit(MessageBase):
    typename = COMMIT
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
    )


class Checkpoint(MessageBase):
    typename = CHECKPOINT
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),
        (f.SEQ_NO_END.nm, NonNegativeNumberField()),
        (f.DIGEST.nm, NonEmptyStringField()),
    )


class ThreePCState(MessageBase):
    typename = THREE_PC_STATE
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.MSGS.nm, IterableField(ClientMessageValidator(operation_schema_is_strict=True))),
    )

# TODO implement actual rules
class CheckpointState(MessageBase):
    typename = CHECKPOINT_STATE
    schema = (
        (f.SEQ_NO.nm, AnyValueField()),
        (f.DIGESTS.nm, AnyValueField()),
        (f.DIGEST.nm, AnyValueField()),
        (f.RECEIVED_DIGESTS.nm, AnyValueField()),
        (f.IS_STABLE.nm, AnyValueField())
    )

# TODO implement actual rules
class Reply(MessageBase):
    typename = REPLY
    schema = (
        (f.RESULT.nm, AnyValueField()),
    )


class InstanceChange(MessageBase):
    typename = INSTANCE_CHANGE
    schema = (
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.REASON.nm, NonNegativeNumberField())
    )


class LedgerStatus(MessageBase):
    typename = LEDGER_STATUS
    schema = (
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.TXN_SEQ_NO.nm, NonNegativeNumberField()),
        (f.MERKLE_ROOT.nm, MerkleRootField()),
    )


class ConsistencyProof(MessageBase):
    typename = CONSISTENCY_PROOF
    schema = (
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),
        (f.SEQ_NO_END.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.OLD_MERKLE_ROOT.nm, MerkleRootField()),
        (f.NEW_MERKLE_ROOT.nm, MerkleRootField()),
        (f.HASHES.nm, IterableField(NonEmptyStringField())),
    )

# TODO: Catchup is not a good name, replace it with `sync` or something which
# is familiar

class CatchupReq(MessageBase):
    typename = CATCHUP_REQ
    schema = (
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),
        (f.SEQ_NO_END.nm, NonNegativeNumberField()),
        (f.CATCHUP_TILL.nm, NonNegativeNumberField()),
    )

class CatchupRep(MessageBase):

    typename = CATCHUP_REP
    schema = (
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.TXNS.nm, MapField(key_field=StringifiedNonNegativeNumberField(),
                             value_field=ClientMessageValidator(operation_schema_is_strict=False))),
        (f.CONS_PROOF.nm, IterableField(Base58Field(byte_lengths=(32,)))),
    )


# TODO implement actual rules if it is required
class ConsProofRequest(MessageBase):
    typename = CONS_PROOF_REQUEST
    schema = (
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),
        (f.SEQ_NO_END.nm, NonNegativeNumberField()),
    )


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

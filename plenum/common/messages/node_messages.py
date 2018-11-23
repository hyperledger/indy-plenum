from typing import TypeVar, NamedTuple

from plenum.common.constants import NOMINATE, BATCH, REELECTION, PRIMARY, \
    BLACKLIST, REQACK, REQNACK, REJECT, \
    POOL_LEDGER_TXNS, ORDERED, PROPAGATE, PREPREPARE, PREPARE, COMMIT, \
    CHECKPOINT, THREE_PC_STATE, CHECKPOINT_STATE, \
    REPLY, INSTANCE_CHANGE, LEDGER_STATUS, CONSISTENCY_PROOF, CATCHUP_REQ, \
    CATCHUP_REP, VIEW_CHANGE_DONE, CURRENT_STATE, \
    MESSAGE_REQUEST, MESSAGE_RESPONSE, OBSERVED_DATA, BATCH_COMMITTED, OPERATION_SCHEMA_IS_STRICT, \
    BACKUP_INSTANCE_FAULTY, VIEW_CHANGE_START, PROPOSED_VIEW_NO, VIEW_CHANGE_CONTINUE
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.messages.fields import NonNegativeNumberField, IterableField, \
    SerializedValueField, SignatureField, TieAmongField, AnyValueField, TimestampField, \
    LedgerIdField, MerkleRootField, Base58Field, LedgerInfoField, AnyField, ChooseField, AnyMapField, \
    LimitedLengthStringField, BlsMultiSignatureField, ProtocolVersionField, BooleanField, \
    IntegerField
from plenum.common.messages.message_base import \
    MessageBase
from plenum.common.types import f
from plenum.config import NAME_FIELD_LIMIT, DIGEST_FIELD_LIMIT, SENDER_CLIENT_FIELD_LIMIT, HASH_FIELD_LIMIT, \
    SIGNATURE_FIELD_LIMIT, TIE_IDR_FIELD_LIMIT, BLS_SIG_LIMIT


class Nomination(MessageBase):
    typename = NOMINATE

    schema = (
        (f.NAME.nm, LimitedLengthStringField(max_length=NAME_FIELD_LIMIT)),
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.ORD_SEQ_NO.nm, NonNegativeNumberField()),
    )


class Batch(MessageBase):
    typename = BATCH

    schema = (
        (f.MSGS.nm, IterableField(SerializedValueField())),
        (f.SIG.nm, SignatureField(max_length=SIGNATURE_FIELD_LIMIT)),
    )


class Reelection(MessageBase):
    typename = REELECTION

    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.ROUND.nm, NonNegativeNumberField()),
        (f.TIE_AMONG.nm, IterableField(TieAmongField(max_length=TIE_IDR_FIELD_LIMIT))),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
    )


class Primary(MessageBase):
    typename = PRIMARY

    schema = (
        (f.NAME.nm, LimitedLengthStringField(max_length=NAME_FIELD_LIMIT)),
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
        (f.VALID_REQ_IDR.nm, IterableField(LimitedLengthStringField(
            max_length=DIGEST_FIELD_LIMIT))),
        (f.INVALID_REQ_IDR.nm, IterableField(LimitedLengthStringField(
            max_length=DIGEST_FIELD_LIMIT))),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.PP_TIME.nm, TimestampField()),
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.STATE_ROOT.nm, MerkleRootField(nullable=True)),
        (f.TXN_ROOT.nm, MerkleRootField(nullable=True)),
        (f.PLUGIN_FIELDS.nm, AnyMapField(optional=True, nullable=True))
    )


class Propagate(MessageBase):
    typename = PROPAGATE
    schema = (
        (f.REQUEST.nm, ClientMessageValidator(
            operation_schema_is_strict=OPERATION_SCHEMA_IS_STRICT)),
        (f.SENDER_CLIENT.nm, LimitedLengthStringField(max_length=SENDER_CLIENT_FIELD_LIMIT, nullable=True)),
    )


class PrePrepare(MessageBase):
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.PP_TIME.nm, TimestampField()),
        (f.REQ_IDR.nm, IterableField(LimitedLengthStringField(
            max_length=DIGEST_FIELD_LIMIT))),
        (f.DISCARDED.nm, SerializedValueField(nullable=True)),
        (f.DIGEST.nm, LimitedLengthStringField(max_length=DIGEST_FIELD_LIMIT)),
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.STATE_ROOT.nm, MerkleRootField(nullable=True)),
        (f.TXN_ROOT.nm, MerkleRootField(nullable=True)),
        (f.SUB_SEQ_NO.nm, NonNegativeNumberField()),
        (f.FINAL.nm, BooleanField()),
        (f.POOL_STATE_ROOT_HASH.nm, MerkleRootField(optional=True,
                                                    nullable=True)),
        # TODO: support multiple multi-sigs for multiple previous batches
        (f.BLS_MULTI_SIG.nm, BlsMultiSignatureField(optional=True,
                                                    nullable=True)),
        (f.PLUGIN_FIELDS.nm, AnyMapField(optional=True, nullable=True)),
    )
    typename = PREPREPARE


class Prepare(MessageBase):
    typename = PREPARE
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.PP_TIME.nm, TimestampField()),
        (f.DIGEST.nm, LimitedLengthStringField(max_length=DIGEST_FIELD_LIMIT)),
        (f.STATE_ROOT.nm, MerkleRootField(nullable=True)),
        (f.TXN_ROOT.nm, MerkleRootField(nullable=True)),
        (f.PLUGIN_FIELDS.nm, AnyMapField(optional=True, nullable=True))
    )


class Commit(MessageBase):
    typename = COMMIT
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.BLS_SIG.nm, LimitedLengthStringField(max_length=BLS_SIG_LIMIT,
                                                optional=True)),
        # PLUGIN_FIELDS is not used in Commit as of now but adding for
        # consistency
        (f.PLUGIN_FIELDS.nm, AnyMapField(optional=True, nullable=True))
    )


class Checkpoint(MessageBase):
    typename = CHECKPOINT
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),
        (f.SEQ_NO_END.nm, NonNegativeNumberField()),
        (f.DIGEST.nm, LimitedLengthStringField(max_length=DIGEST_FIELD_LIMIT)),
    )


class ThreePCState(MessageBase):
    typename = THREE_PC_STATE
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.MSGS.nm, IterableField(ClientMessageValidator(
            operation_schema_is_strict=OPERATION_SCHEMA_IS_STRICT))),
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


class BackupInstanceFaulty(MessageBase):
    typename = BACKUP_INSTANCE_FAULTY
    schema = (
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.INSTANCES.nm, IterableField(NonNegativeNumberField())),
        (f.REASON.nm, NonNegativeNumberField())
    )


class LedgerStatus(MessageBase):
    """
    Purpose: spread status of ledger copy on a specific node.
    When node receives this message and see that it has different
    status of ledger it should reply with LedgerStatus that contains its
    status
    """
    typename = LEDGER_STATUS
    schema = (
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.TXN_SEQ_NO.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField(nullable=True)),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField(nullable=True)),
        (f.MERKLE_ROOT.nm, MerkleRootField()),
        (f.PROTOCOL_VERSION.nm, ProtocolVersionField())
    )


class ConsistencyProof(MessageBase):
    typename = CONSISTENCY_PROOF
    schema = (
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),
        (f.SEQ_NO_END.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.OLD_MERKLE_ROOT.nm, MerkleRootField()),
        (f.NEW_MERKLE_ROOT.nm, MerkleRootField()),
        (f.HASHES.nm, IterableField(LimitedLengthStringField(max_length=HASH_FIELD_LIMIT))),
    )


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
        # TODO: turn on validation, the cause is INDY-388
        # (f.TXNS.nm, MapField(key_field=StringifiedNonNegativeNumberField(),
        #                      value_field=ClientMessageValidator(operation_schema_is_strict=False))),
        (f.TXNS.nm, AnyValueField()),
        (f.CONS_PROOF.nm, IterableField(Base58Field(byte_lengths=(32,)))),
    )


class ViewChangeDone(MessageBase):
    """
    Node sends this kind of message when view change steps done and it is
    ready to switch to the new primary.
    In contrast to 'Primary' message this one does not imply election.
    """
    typename = VIEW_CHANGE_DONE

    schema = (
        # name is nullable because this message can be sent when
        # there were no view changes and instance has no primary yet
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.NAME.nm, LimitedLengthStringField(max_length=NAME_FIELD_LIMIT,
                                             nullable=True)),
        (f.LEDGER_INFO.nm, IterableField(LedgerInfoField()))
    )


class CurrentState(MessageBase):
    """
    Node sends this kind of message for nodes which
    suddenly reconnected (lagged). It contains information about current
    pool state, like view no, primary etc.
    """
    typename = CURRENT_STATE

    schema = (
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PRIMARY.nm, IterableField(AnyField())),  # ViewChangeDone
    )


"""
The choice to do a generic 'request message' feature instead of a specific
one was debated. It has some pros and some cons. We wrote up the analysis in
http://bit.ly/2uxf6Se. This decision can and should be revisited if we feel a
lot of ongoing dissonance about it. Lovesh, Alex, and Daniel, July 2017
"""


class MessageReq(MessageBase):
    """
    Purpose: ask node for any message
    """
    allowed_types = {LEDGER_STATUS, CONSISTENCY_PROOF, PREPREPARE, PREPARE,
                     COMMIT, PROPAGATE}
    typename = MESSAGE_REQUEST
    schema = (
        (f.MSG_TYPE.nm, ChooseField(values=allowed_types)),
        (f.PARAMS.nm, AnyMapField())
    )


class MessageRep(MessageBase):
    """
    Purpose: respond to a node for any requested message
    """
    # TODO: support a setter for `msg` to create an instance of a type
    # according to `msg_type`
    typename = MESSAGE_RESPONSE
    schema = (
        (f.MSG_TYPE.nm, ChooseField(values=MessageReq.allowed_types)),
        (f.PARAMS.nm, AnyMapField()),
        (f.MSG.nm, AnyField())
    )


ThreePhaseType = (PrePrepare, Prepare, Commit)
ThreePhaseMsg = TypeVar("3PhaseMsg", *ThreePhaseType)

ElectionType = (Nomination, Primary, Reelection)
ElectionMsg = TypeVar("ElectionMsg", *ElectionType)

ThreePhaseKey = NamedTuple("ThreePhaseKey", [
    f.VIEW_NO,
    f.PP_SEQ_NO
])


class BatchCommitted(MessageBase):
    """
    Purpose: pass to Observable after each batch is committed
    (so that Observable can propagate the data to Observers using ObservedData msg)
    """
    typename = BATCH_COMMITTED
    schema = (
        (f.REQUESTS.nm,
         IterableField(ClientMessageValidator(
             operation_schema_is_strict=OPERATION_SCHEMA_IS_STRICT))),
        (f.LEDGER_ID.nm, LedgerIdField()),
        (f.PP_TIME.nm, TimestampField()),
        (f.STATE_ROOT.nm, MerkleRootField()),
        (f.TXN_ROOT.nm, MerkleRootField()),
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),
        (f.SEQ_NO_END.nm, NonNegativeNumberField())
    )


class ObservedData(MessageBase):
    """
    Purpose: propagate data from Validators to Observers
    """
    # TODO: support other types
    # TODO: support validation of Msg according to the type
    allowed_types = {BATCH}
    typename = OBSERVED_DATA
    schema = (
        (f.MSG_TYPE.nm, ChooseField(values=allowed_types)),
        (f.MSG.nm, AnyValueField())
    )

    def _validate_message(self, dct):
        msg = dct[f.MSG.nm]
        # TODO: support other types
        expected_type_cls = BatchCommitted
        if isinstance(msg, expected_type_cls):
            return None
        if isinstance(msg, dict):
            expected_type_cls(**msg)
            return None
        self._raise_invalid_fields(
            f.MSG.nm, msg,
            "The message type must be {} ".format(expected_type_cls.typename))


class FutureViewChangeDone:
    """
    Purpose: sent from Node to ViewChanger to indicate that other nodes finished ViewChange to one of the next view
    In particular, it's sent when CURRENT_STATE (with primary propagation) is processed.
    """
    def __init__(self, vcd_msg: ViewChangeDone, from_current_state: bool) -> None:
        self.vcd_msg = vcd_msg
        self.from_current_state = from_current_state


class ViewChangeStartMessage(MessageBase):
    typename = VIEW_CHANGE_START
    schema = (
        (PROPOSED_VIEW_NO, IntegerField()),
    )


class ViewChangeContinueMessage(MessageBase):
    typename = VIEW_CHANGE_CONTINUE
    schema = (
        (PROPOSED_VIEW_NO, IntegerField()),
    )

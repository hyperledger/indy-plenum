from typing import TypeVar, NamedTuple, Dict

from plenum.common.constants import BATCH, BLACKLIST, REQACK, REQNACK, REJECT, \
    POOL_LEDGER_TXNS, ORDERED, PROPAGATE, PREPREPARE, PREPARE, COMMIT, CHECKPOINT, \
    REPLY, INSTANCE_CHANGE, LEDGER_STATUS, CONSISTENCY_PROOF, CATCHUP_REQ, CATCHUP_REP, \
    VIEW_CHANGE_DONE, CURRENT_STATE, MESSAGE_REQUEST, MESSAGE_RESPONSE, OBSERVED_DATA, \
    BATCH_COMMITTED, OPERATION_SCHEMA_IS_STRICT, BACKUP_INSTANCE_FAULTY, VIEW_CHANGE_START, \
    PROPOSED_VIEW_NO, VIEW_CHANGE_CONTINUE, VIEW_CHANGE, VIEW_CHANGE_ACK, NEW_VIEW, \
    OLD_VIEW_PREPREPARE_REQ, OLD_VIEW_PREPREPARE_REP
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.messages.fields import NonNegativeNumberField, IterableField, \
    SerializedValueField, SignatureField, AnyValueField, TimestampField, \
    LedgerIdField, MerkleRootField, Base58Field, LedgerInfoField, AnyField, ChooseField, AnyMapField, \
    LimitedLengthStringField, BlsMultiSignatureField, ProtocolVersionField, BooleanField, \
    IntegerField, BatchIDField, ViewChangeField, MapField, StringifiedNonNegativeNumberField
from plenum.common.messages.message_base import MessageBase
from plenum.common.types import f
from plenum.config import NAME_FIELD_LIMIT, DIGEST_FIELD_LIMIT, SENDER_CLIENT_FIELD_LIMIT, HASH_FIELD_LIMIT, \
    SIGNATURE_FIELD_LIMIT, BLS_SIG_LIMIT


# TODO set of classes are not hashable but MessageBase expects that
from plenum.server.consensus.batch_id import BatchID


class Batch(MessageBase):
    typename = BATCH

    schema = (
        (f.MSGS.nm, IterableField(SerializedValueField())),
        (f.SIG.nm, SignatureField(max_length=SIGNATURE_FIELD_LIMIT)),
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
        (f.AUDIT_TXN_ROOT_HASH.nm, MerkleRootField(nullable=True)),
        (f.PRIMARIES.nm, IterableField(LimitedLengthStringField(
            max_length=NAME_FIELD_LIMIT))),
        (f.NODE_REG.nm, IterableField(LimitedLengthStringField(
            max_length=NAME_FIELD_LIMIT))),
        (f.ORIGINAL_VIEW_NO.nm, NonNegativeNumberField()),
        (f.DIGEST.nm, LimitedLengthStringField(max_length=DIGEST_FIELD_LIMIT)),
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
        (f.AUDIT_TXN_ROOT_HASH.nm, MerkleRootField(optional=True,
                                                   nullable=True)),
        # TODO: support multiple multi-sigs for multiple previous batches
        (f.BLS_MULTI_SIG.nm, BlsMultiSignatureField(optional=True,
                                                    nullable=True)),
        (f.BLS_MULTI_SIGS.nm, IterableField(optional=True,
                                            inner_field_type=BlsMultiSignatureField(optional=True, nullable=True))),
        (f.ORIGINAL_VIEW_NO.nm, NonNegativeNumberField(optional=True,
                                                       nullable=True)),
        (f.PLUGIN_FIELDS.nm, AnyMapField(optional=True, nullable=True)),
    )
    typename = PREPREPARE

    def _post_process(self, input_as_dict: Dict) -> Dict:
        # make validated input hashable
        input_as_dict[f.REQ_IDR.nm] = tuple(input_as_dict[f.REQ_IDR.nm])

        bls = input_as_dict.get(f.BLS_MULTI_SIG.nm, None)
        if bls is not None:
            input_as_dict[f.BLS_MULTI_SIG.nm] = (bls[0], tuple(bls[1]), tuple(bls[2]))

        bls_sigs = input_as_dict.get(f.BLS_MULTI_SIGS.nm, None)
        if bls_sigs is not None:
            sub = []
            for sig in bls_sigs:
                sub.append((sig[0], tuple(sig[1]), tuple(sig[2])))
            input_as_dict[f.BLS_MULTI_SIGS.nm] = tuple(sub)

        return input_as_dict


# TODO: use generic MessageReq mechanism once it's separated into an independent service
class OldViewPrePrepareRequest(MessageBase):
    typename = OLD_VIEW_PREPREPARE_REQ
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.BATCH_IDS.nm, IterableField(BatchIDField())),
    )


class OldViewPrePrepareReply(MessageBase):
    typename = OLD_VIEW_PREPREPARE_REP
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.PREPREPARES.nm, IterableField(AnyField())),
    )


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
        (f.AUDIT_TXN_ROOT_HASH.nm, MerkleRootField(optional=True,
                                                   nullable=True)),
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
        (f.BLS_SIGS.nm, MapField(optional=True,
                                 key_field=StringifiedNonNegativeNumberField(),
                                 value_field=LimitedLengthStringField(max_length=BLS_SIG_LIMIT))),
        # PLUGIN_FIELDS is not used in Commit as of now but adding for
        # consistency
        (f.PLUGIN_FIELDS.nm, AnyMapField(optional=True, nullable=True)),
    )


class Checkpoint(MessageBase):
    typename = CHECKPOINT
    schema = (
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),  # This will no longer be used soon
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),  # This is no longer used and must always be 0
        (f.SEQ_NO_END.nm, NonNegativeNumberField()),
        (f.DIGEST.nm, MerkleRootField(nullable=True)),  # This is actually audit ledger merkle root
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

#
# class CheckpointsList(IterableField):
#
#     def __init__(self, min_length=None, max_length=None, **kwargs):
#         super().__init__(AnyField(), min_length, max_length, **kwargs)
#
#     def _specific_validation(self, val):
#         result = super()._specific_validation(val)
#         if result is not None:
#             return result
#         for chk in val:
#             if not isinstance(chk, Checkpoint):
#                 return "Checkpoints list contains not Checkpoint objects"


class ViewChange(MessageBase):
    typename = VIEW_CHANGE
    schema = (
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.STABLE_CHECKPOINT.nm, NonNegativeNumberField()),
        (f.PREPARED.nm, IterableField(BatchIDField())),  # list of tuples (view_no, pp_view_no, pp_seq_no, pp_digest)
        (f.PREPREPARED.nm, IterableField(BatchIDField())),  # list of tuples (view_no, pp_view_no, pp_seq_no, pp_digest)
        (f.CHECKPOINTS.nm, IterableField(AnyField()))  # list of Checkpoints TODO: should we change to tuples?
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        checkpoints = []
        for chk in self.checkpoints:
            if isinstance(chk, dict):
                checkpoints.append(Checkpoint(**chk))
        if checkpoints:
            self.checkpoints = checkpoints

        # The field `prepared` can to be a list of BatchIDs or of dicts.
        # If its a list of dicts then we need to deserialize it.
        if self.prepared and isinstance(self.prepared[0], dict):
            self.prepared = [BatchID(**bid)
                             for bid in self.prepared
                             if isinstance(bid, dict)]
        # The field `preprepared` can to be a list of BatchIDs or of dicts.
        # If its a list of dicts then we need to deserialize it.
        if self.preprepared and isinstance(self.preprepared[0], dict):
            self.preprepared = [BatchID(**bid)
                                for bid in self.preprepared
                                if isinstance(bid, dict)]

    def _asdict(self):
        result = super()._asdict()
        checkpoints = []
        for chk in self.checkpoints:
            if isinstance(chk, dict):
                continue
            checkpoints.append(chk._asdict())
        if checkpoints:
            result[f.CHECKPOINTS.nm] = checkpoints
        # The field `prepared` can to be a list of BatchIDs or of dicts.
        # If its a list of BatchID then we need to serialize it.
        if self.prepared and isinstance(self.prepared[0], BatchID):
            result[f.PREPARED.nm] = [bid._asdict()
                                     for bid in self.prepared]
        # The field `preprepared` can to be a list of BatchIDs or of dicts.
        # If its a list of BatchID then we need to serialize it.
        if self.preprepared and isinstance(self.preprepared[0], BatchID):
            result[f.PREPREPARED.nm] = [bid._asdict()
                                        for bid in self.preprepared]
        return result


class ViewChangeAck(MessageBase):
    typename = VIEW_CHANGE_ACK
    schema = (
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.NAME.nm, LimitedLengthStringField(max_length=NAME_FIELD_LIMIT)),
        (f.DIGEST.nm, LimitedLengthStringField(max_length=DIGEST_FIELD_LIMIT))
    )


class NewView(MessageBase):
    typename = NEW_VIEW
    schema = (
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.VIEW_CHANGES.nm, IterableField(ViewChangeField())),  # list of tuples (node_name, view_change_digest)
        (f.CHECKPOINT.nm, AnyField()),  # Checkpoint to be selected as stable (TODO: or tuple?)
        (f.BATCHES.nm, IterableField(BatchIDField())),  # list of tuples (view_no, pp_view_no, pp_seq_no, pp_digest)
        (f.PRIMARY.nm, LimitedLengthStringField(optional=True))
        # that should get into new view
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if isinstance(self.checkpoint, dict):
            self.checkpoint = Checkpoint(**self.checkpoint)
        # The field `batches` can to be a list of BatchIDs or of dicts.
        # If it's not a list of dicts then we don't need to deserialize it.
        if not self.batches or not isinstance(self.batches[0], dict):
            return
        self.batches = [BatchID(**bid)
                        for bid in self.batches
                        if isinstance(bid, dict)]

    def _asdict(self):
        result = super()._asdict()
        chk = self.checkpoint
        if not isinstance(chk, dict):
            result[f.CHECKPOINT.nm] = chk._asdict()
        # The field `batches` can to be a list of BatchIDs or of dicts.
        # If its a list of dicts then we don't need to serialize it.
        if not self.batches or not isinstance(self.batches[0], BatchID):
            return result
        result[f.BATCHES.nm] = [bid._asdict()
                                for bid in self.batches]
        return result


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
                     COMMIT, PROPAGATE, VIEW_CHANGE, NEW_VIEW}
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
        (f.INST_ID.nm, NonNegativeNumberField()),
        (f.VIEW_NO.nm, NonNegativeNumberField()),
        (f.PP_SEQ_NO.nm, NonNegativeNumberField()),
        (f.PP_TIME.nm, TimestampField()),
        (f.STATE_ROOT.nm, MerkleRootField()),
        (f.TXN_ROOT.nm, MerkleRootField()),
        (f.SEQ_NO_START.nm, NonNegativeNumberField()),
        (f.SEQ_NO_END.nm, NonNegativeNumberField()),
        (f.AUDIT_TXN_ROOT_HASH.nm, MerkleRootField(nullable=True)),
        (f.PRIMARIES.nm, IterableField(LimitedLengthStringField(
            max_length=NAME_FIELD_LIMIT))),
        (f.NODE_REG.nm, IterableField(LimitedLengthStringField(
            max_length=NAME_FIELD_LIMIT))),
        (f.ORIGINAL_VIEW_NO.nm, NonNegativeNumberField()),
        (f.DIGEST.nm, LimitedLengthStringField(max_length=DIGEST_FIELD_LIMIT)),
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

    def __init__(self, vcd_msg: ViewChangeDone) -> None:
        self.vcd_msg = vcd_msg


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

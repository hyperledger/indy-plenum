from typing import Dict, Any, Optional
from abc import ABCMeta, abstractmethod

from plenum.common.exceptions import (
    MismatchedMessageReplyException, MissingNodeOp, InvalidNodeOp
)
from plenum.common.constants import (
    OP_FIELD_NAME, PROPAGATE, COMMIT, PREPREPARE, PREPARE,
    LEDGER_STATUS, CONSISTENCY_PROOF
)
from plenum.common.messages.message_base import MessageBase
from plenum.common.messages.node_messages import MessageReq, MessageRep, \
    LedgerStatus, PrePrepare, ConsistencyProof, Propagate, Prepare, Commit
from plenum.common.messages.node_message_factory import node_message_factory
from plenum.common.txn_util import TxnUtilConfig
from plenum.common.types import f
from plenum.server import replica
from stp_core.common.log import getlogger


logger = getlogger()


class BaseHandler(metaclass=ABCMeta):

    # TODO use just list of names from "plenum.common.types.f"
    # instead of Map that is expected here now
    fields = NotImplemented
    typename = NotImplemented

    def __init__(self, node):
        self.node = node

    @abstractmethod
    def _validate(self, params: Dict[str, Any]) -> bool:
        pass

    def _check(self, inner_msg: MessageBase, params: Dict[str, Any]) -> Any:
        for field_name, type_name in self.fields.items():
            if (not hasattr(inner_msg, type_name) or
                    (getattr(inner_msg, type_name) != params[field_name])):
                raise MismatchedMessageReplyException

    @abstractmethod
    def _requestor(self, params: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def _processor(self, inner_msg: MessageBase) -> None:
        pass

    def serve(self, msg: MessageReq):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self._validate(params):
            self.node.discard(msg, 'cannot serve request',
                              logMethod=logger.debug)
            return None

        return self._requestor(params)

    def process(self, msg: MessageRep):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self._validate(params):
            self.node.discard(msg, 'cannot process message reply',
                              logMethod=logger.debug)
            return

        try:
            # TODO msg.ts_rcv doesn't make sense here for inner message (msg.msg)
            # thus it will impact logic of PrePrepare obsolescence
            kwargs = msg.msg
            kwargs[OP_FIELD_NAME] = self.typename
            inner_msg = node_message_factory.get_instance(**kwargs, frm=msg.frm, ts_rcv=None)
        except (MissingNodeOp, InvalidNodeOp, TypeError) as ex:
            self.node.discard(msg, 'replied message has invalid structure',
                              logMethod=logger.warning)
        else:
            try:
                # check that inner message has the same values as in requested params
                self._check(inner_msg, params)
            except MismatchedMessageReplyException:
                self.node.discard(msg, 'replied message does not satisfy query criteria',
                                  logMethod=logger.warning)
            else:
                self._processor(inner_msg)


class LedgerStatusHandler(BaseHandler):
    typename = LEDGER_STATUS
    fields = {
        'ledger_id': f.LEDGER_ID.nm
    }

    def _validate(self, params: Dict[str, Any]) -> bool:
        return params['ledger_id'] in self.node.ledger_ids

    def _requestor(self, params: Dict[str, Any]) -> LedgerStatus:
        return self.node.getLedgerStatus(params['ledger_id'])

    def _processor(self, inner_msg: LedgerStatus) -> None:
        self.node.ledgerManager.processLedgerStatus(inner_msg)


class ConsistencyProofHandler(BaseHandler):
    typename = CONSISTENCY_PROOF
    fields = {
        'ledger_id': f.LEDGER_ID.nm,
        'seq_no_start': f.SEQ_NO_START.nm,
        'seq_no_end': f.SEQ_NO_END.nm
    }

    def _validate(self, params: Dict[str, Any]) -> bool:
        return params['ledger_id'] in self.node.ledger_ids and \
            (isinstance(params['seq_no_start'], int) and params[
             'seq_no_start'] > 0) and \
            (isinstance(params['seq_no_end'], int) and params[
             'seq_no_end'] > 0)

    def _requestor(self, params: Dict[str, Any]) -> ConsistencyProof:
        return self.node.ledgerManager._node_seeder._build_consistency_proof(
            params['ledger_id'],
            params['seq_no_start'],
            params['seq_no_end']
        )

    def _processor(self, inner_msg: ConsistencyProof) -> None:
        self.node.ledgerManager.processConsistencyProof(inner_msg)


class PreprepareHandler(BaseHandler):
    typename = PREPREPARE
    fields = {
        'inst_id': f.INST_ID.nm,
        'view_no': f.VIEW_NO.nm,
        'pp_seq_no': f.PP_SEQ_NO.nm
    }
    msg_cls = Prepare

    def _validate(self, params: Dict[str, Any]) -> bool:
        return params['inst_id'] in self.node.replicas.keys() and \
            params['view_no'] == self.node.viewNo and \
            isinstance(params['pp_seq_no'], int) and \
            params['pp_seq_no'] > 0

    def _requestor(self, params: Dict[str, Any]) -> Optional[PrePrepare]:
        return self.node.replicas[params['inst_id']].sentPrePrepares.get((
            params['view_no'], params['pp_seq_no']
        ))

    def _processor(self, inner_msg: PrePrepare) -> None:
        self.node.replicas[inner_msg.instId].process_requested_pre_prepare(
            inner_msg
        )


class PrepareHandler(BaseHandler):
    typename = PREPARE
    fields = {
        'inst_id': f.INST_ID.nm,
        'view_no': f.VIEW_NO.nm,
        'pp_seq_no': f.PP_SEQ_NO.nm
    }

    def _validate(self, params: Dict[str, Any]) -> bool:
        return params['inst_id'] in self.node.replicas.keys() and \
            params['view_no'] == self.node.viewNo and \
            isinstance(params['pp_seq_no'], int) and \
            params['pp_seq_no'] > 0

    def _requestor(self, params: Dict[str, Any]) -> Prepare:
        return self.node.replicas[params['inst_id']].get_sent_prepare(
            params['view_no'], params['pp_seq_no']
        )

    def _processor(self, inner_msg: Prepare) -> None:
        self.node.replicas[inner_msg.instId].process_requested_prepare(
            inner_msg
        )


class CommitHandler(BaseHandler):
    typename = COMMIT
    fields = {
        'inst_id': f.INST_ID.nm,
        'view_no': f.VIEW_NO.nm,
        'pp_seq_no': f.PP_SEQ_NO.nm
    }

    def _validate(self, params: Dict[str, Any]) -> bool:
        return params['inst_id'] in self.node.replicas.keys() and \
            params['view_no'] == self.node.viewNo and \
            isinstance(params['pp_seq_no'], int) and \
            params['pp_seq_no'] > 0

    def _requestor(self, params: Dict[str, Any]) -> Commit:
        return self.node.replicas[params['inst_id']].get_sent_commit(
            params['view_no'], params['pp_seq_no']
        )

    def _processor(self, inner_msg: Commit) -> None:
        self.node.replicas[inner_msg.instId].process_requested_commit(
            inner_msg
        )


class PropagateHandler(BaseHandler):
    typename = PROPAGATE
    fields = {
        'digest': f.DIGEST.nm
    }

    def _validate(self, params: Dict[str, Any]) -> bool:
        return params['digest'] is not None

    def _check(self, inner_msg: Propagate, params: Dict[str, Any]) -> Propagate:
        request = TxnUtilConfig.client_request_class(**inner_msg.request)
        if request.digest != params['digest']:
            raise MismatchedMessageReplyException

    def _requestor(self, params: Dict[str, Any]) -> Optional[Propagate]:
        req_key = params[f.DIGEST.nm]
        if req_key in self.node.requests and self.node.requests[req_key].finalised:
            sender_client = self.node.requestSender.get(req_key)
            req = self.node.requests[req_key].finalised
            return self.node.createPropagate(req, sender_client)
        return None

    def _processor(self, inner_msg: Propagate) -> None:
        self.node.processPropagate(inner_msg)

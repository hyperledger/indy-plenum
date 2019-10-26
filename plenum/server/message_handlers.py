import logging
from typing import Dict, Any, Optional, Tuple, Callable
from abc import ABCMeta, abstractmethod

from plenum.common.constants import THREE_PC_PREFIX
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.exceptions import MismatchedMessageReplyException
from plenum.common.messages.message_base import MessageBase
from plenum.common.messages.node_messages import MessageReq, MessageRep, \
    LedgerStatus, PrePrepare, ConsistencyProof, Propagate, Prepare, Commit
from plenum.common.txn_util import TxnUtilConfig
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from stp_core.common.log import getlogger


logger = getlogger()


class BaseHandler(metaclass=ABCMeta):

    fields = NotImplemented

    def __init__(self, node):
        self.node = node

    @abstractmethod
    def validate(self, **kwargs) -> bool:
        pass

    @abstractmethod
    def create(self, msg: Dict, **kwargs) -> Any:
        pass

    @abstractmethod
    def requestor(self, params: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def processor(self, validated_msg: object, params: Dict[str, Any], frm: str) -> Optional:
        pass

    def serve(self, msg: MessageReq):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self.validate(**params):
            self.discard(msg, 'cannot serve request',
                              logMethod=logger.debug)
            return None

        return self.requestor(params)

    def process(self, msg: MessageRep, frm: str):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self.validate(**params):
            self.discard(msg, 'cannot process message reply',
                              logMethod=logger.debug)
            return

        try:
            valid_msg = self.create(msg.msg, **params)
            self.processor(valid_msg, params, frm)
        except TypeError:
            self.discard(msg, 'replied message has invalid structure',
                              logMethod=logger.warning)
        except MismatchedMessageReplyException:
            self.discard(msg, 'replied message does not satisfy query criteria',
                              logMethod=logger.warning)

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        """
        Discard a message and log a reason using the specified `logMethod`.

        :param msg: the message to discard
        :param reason: the reason why this message is being discarded
        :param logMethod: the logging function to be used
        :param cliOutput: if truthy, informs a CLI that the logged msg should
        be printed
        """
        self.node.discard(msg, reason, logMethod, cliOutput)


class LedgerStatusHandler(BaseHandler):
    fields = {
        'ledger_id': f.LEDGER_ID.nm
    }

    def validate(self, **kwargs) -> bool:
        return kwargs['ledger_id'] in self.node.ledger_ids

    def create(self, msg: Dict, **kwargs) -> LedgerStatus:
        ls = LedgerStatus(**msg)
        if ls.ledgerId != kwargs['ledger_id']:
            raise MismatchedMessageReplyException
        return ls

    def requestor(self, params: Dict[str, Any]) -> LedgerStatus:
        return self.node.getLedgerStatus(params['ledger_id'])

    def processor(self, validated_msg: LedgerStatus, params: Dict[str, Any], frm: str) -> None:
        self.node.ledgerManager.processLedgerStatus(validated_msg, frm=frm)


class ConsistencyProofHandler(BaseHandler):
    fields = {
        'ledger_id': f.LEDGER_ID.nm,
        'seq_no_start': f.SEQ_NO_START.nm,
        'seq_no_end': f.SEQ_NO_END.nm
    }

    def validate(self, **kwargs) -> bool:
        return kwargs['ledger_id'] in self.node.ledger_ids and \
            (isinstance(kwargs['seq_no_start'], int) and kwargs[
             'seq_no_start'] > 0) and \
            (isinstance(kwargs['seq_no_end'], int) and kwargs[
             'seq_no_end'] > 0)

    def create(self, msg: Dict, **kwargs) -> ConsistencyProof:
        cp = ConsistencyProof(**msg)
        if cp.ledgerId != kwargs['ledger_id'] \
                or cp.seqNoStart != kwargs['seq_no_start'] \
                or cp.seqNoEnd != kwargs['seq_no_end']:
            raise MismatchedMessageReplyException
        return cp

    def requestor(self, params: Dict[str, Any]) -> ConsistencyProof:
        return self.node.ledgerManager._node_seeder._build_consistency_proof(
            params['ledger_id'],
            params['seq_no_start'],
            params['seq_no_end'])

    def processor(self, validated_msg: ConsistencyProof, params: Dict[str, Any], frm: str) -> None:
        self.node.ledgerManager.processConsistencyProof(validated_msg, frm=frm)


class PropagateHandler(BaseHandler):
    fields = {
        'digest': f.DIGEST.nm
    }

    def validate(self, **kwargs) -> bool:
        return kwargs['digest'] is not None

    def create(self, msg: Dict, **kwargs) -> Propagate:
        ppg = Propagate(**msg)
        request = TxnUtilConfig.client_request_class(**ppg.request)
        if request.digest != kwargs['digest']:
            raise MismatchedMessageReplyException
        return ppg

    def requestor(self, params: Dict[str, Any]) -> Optional[Propagate]:
        req_key = params[f.DIGEST.nm]
        if req_key in self.node.requests:
            sender_client = self.node.requestSender.get(req_key)
            req = self.node.requests[req_key].request
            return self.node.createPropagate(req, sender_client)
        return None

    def processor(self, validated_msg: Propagate, params: Dict[str, Any], frm: str) -> None:
        self.node.processPropagate(validated_msg, frm)

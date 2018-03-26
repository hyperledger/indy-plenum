from typing import Dict, Any, Optional
from abc import ABCMeta, abstractmethod

from plenum.common.constants import THREE_PC_PREFIX
from plenum.common.messages.fields import RequestIdentifierField
from plenum.common.messages.node_messages import MessageReq, MessageRep, \
    LedgerStatus, PrePrepare, ConsistencyProof, Propagate, Prepare
from plenum.common.types import f
from plenum.server import replica
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
    def processor(self, validated_msg: object, params: Dict[str, Any], frm: str) -> None:
        pass

    def serve(self, msg: MessageReq):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self.validate(**params):
            self.node.discard(msg, 'cannot serve request', logMethod=logger.debug)
            return None

        return self.requestor(params)

    def process(self, msg: MessageRep, frm: str):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self.validate(**params):
            self.node.discard(msg, 'cannot process requested message response',
                              logMethod=logger.debug)
            return None

        valid_msg = self.create(msg.msg, **params)
        return self.processor(valid_msg, params, frm)


class LedgerStatusHandler(BaseHandler):
    fields = {
        'ledger_id': f.LEDGER_ID.nm
    }

    def validate(self, **kwargs) -> bool:
        return kwargs['ledger_id'] in self.node.ledger_ids

    def create(self, msg: Dict, **kwargs) -> LedgerStatus:
        return LedgerStatus(**msg)

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
        return ConsistencyProof(**msg)

    def requestor(self, params: Dict[str, Any]) -> ConsistencyProof:
        return self.node.ledgerManager._buildConsistencyProof(
            params['ledger_id'],
            params['seq_no_start'],
            params['seq_no_end'])

    def processor(self, validated_msg: ConsistencyProof, params: Dict[str, Any], frm: str) -> None:
        self.node.ledgerManager.processConsistencyProof(validated_msg, frm=frm)


class PreprepareHandler(BaseHandler):
    fields = {
        'inst_id': f.INST_ID.nm,
        'view_no': f.VIEW_NO.nm,
        'pp_seq_no': f.PP_SEQ_NO.nm
    }

    def validate(self, **kwargs) -> bool:
        return kwargs['inst_id'] in range(len(self.node.replicas)) and \
            kwargs['view_no'] == self.node.viewNo and \
            isinstance(kwargs['pp_seq_no'], int) and \
            kwargs['pp_seq_no'] > 0

    def create(self, msg: Dict, **kwargs) -> Optional[PrePrepare]:
        pp = PrePrepare(**msg)
        if pp.instId != kwargs['inst_id'] or pp.viewNo != kwargs['view_no']:
            logger.warning(
                '{}{} found PREPREPARE {} not satisfying query criteria' .format(
                    THREE_PC_PREFIX, self, pp))
            return None
        return pp

    def requestor(self, params: Dict[str, Any]) -> Optional[PrePrepare]:
        return self.node.replicas[params['inst_id']].getPrePrepare(
            params['view_no'], params['pp_seq_no'])

    def processor(self, validated_msg: PrePrepare, params: Dict[str, Any], frm: str) -> None:
        inst_id = params['inst_id']
        frm = replica.Replica.generateName(frm, inst_id)
        self.node.replicas[inst_id].process_requested_pre_prepare(validated_msg,
                                                                  sender=frm)


class PrepareHandler(BaseHandler):
    fields = {
        'inst_id': f.INST_ID.nm,
        'view_no': f.VIEW_NO.nm,
        'pp_seq_no': f.PP_SEQ_NO.nm
    }

    def validate(self, **kwargs) -> bool:
        return kwargs['inst_id'] in range(len(self.node.replicas)) and \
            kwargs['view_no'] == self.node.viewNo and \
            isinstance(kwargs['pp_seq_no'], int) and \
            kwargs['pp_seq_no'] > 0

    def create(self, msg: Dict, **kwargs) -> Optional[Prepare]:
        prepare = Prepare(**msg)
        if prepare.instId != kwargs['inst_id'] or prepare.viewNo != kwargs['view_no']:
            logger.warning(
                '{}{} found PREPARE {} not satisfying query criteria' .format(
                    THREE_PC_PREFIX, self, prepare))
            return None
        return prepare

    def requestor(self, params: Dict[str, Any]) -> Prepare:
        return self.node.replicas[params['inst_id']].get_sent_prepare(
            params['view_no'], params['pp_seq_no'])

    def processor(self, validated_msg: Prepare, params: Dict[str, Any], frm: str) -> None:
        inst_id = params['inst_id']
        frm = replica.Replica.generateName(frm, inst_id)
        self.node.replicas[inst_id].process_requested_prepare(validated_msg,
                                                              sender=frm)


class PropagateHandler(BaseHandler):
    fields = {
        'identifier': f.IDENTIFIER.nm,
        'req_id': f.REQ_ID.nm
    }

    def validate(self, **kwargs) -> bool:
        return not (RequestIdentifierField().validate((kwargs['identifier'],
                    kwargs['req_id'])))

    def create(self, msg: Dict, **kwargs) -> Propagate:
        ppg = Propagate(**msg)
        if ppg.request[f.IDENTIFIER.nm] != kwargs['identifier'] or \
                ppg.request[f.REQ_ID.nm] != kwargs['req_id']:
            logger.debug(
                '{} found PROPAGATE {} not '
                'satisfying query criteria'.format(
                    self, ppg))
            return None
        return ppg

    def requestor(self, params: Dict[str, Any]) -> Optional[Propagate]:
        req_key = (params['identifier'], params['req_id'])
        if req_key in self.node.requests and self.node.requests[req_key].finalised:
            sender_client = self.node.requestSender.get(req_key)
            req = self.node.requests[req_key].finalised
            return self.node.createPropagate(req, sender_client)
        return None

    def processor(self, validated_msg: Propagate, params: Dict[str, Any], frm: str) -> None:
        self.node.processPropagate(validated_msg, frm)

from typing import Dict
from typing import List

from plenum.common.constants import LEDGER_STATUS, PREPREPARE, CONSISTENCY_PROOF, \
    PROPAGATE
from plenum.common.messages.fields import RequestIdentifierField
from plenum.common.messages.node_messages import MessageReq, MessageRep, \
    LedgerStatus, PrePrepare, ConsistencyProof, Propagate
from plenum.common.types import f
from plenum.server import replica
from stp_core.common.log import getlogger


logger = getlogger()


class MessageReqProcessor:
    # This is a mixin, it's mixed with node.
    def __init__(self):
        self.validation_handlers = {
            LEDGER_STATUS: self._validate_requested_ledger_status,
            CONSISTENCY_PROOF: self._validate_requested_cons_proof,
            PREPREPARE: self._validate_requested_preprepare,
            PROPAGATE: self._validate_requested_propagate
        }

        self.req_handlers = {
            LEDGER_STATUS: self._serve_ledger_status_request,
            CONSISTENCY_PROOF: self._serve_cons_proof_request,
            PREPREPARE: self._serve_preprepare_request,
            PROPAGATE: self._serve_propagate_request
        }

        self.rep_handlers = {
            LEDGER_STATUS: self._process_requested_ledger_status,
            CONSISTENCY_PROOF: self._process_requested_cons_proof,
            PREPREPARE: self._process_requested_preprepare,
            PROPAGATE: self._process_requested_propagate
        }

    def process_message_req(self, msg: MessageReq, frm):
        # Assumes a shared memory architecture. In case of multiprocessing,
        # RPC architecture, use deques to communicate the message and node will
        # maintain a unique internal message id to correlate responses.
        msg_type = msg.msg_type
        resp = self.req_handlers[msg_type](msg)

        if resp is False:
            return

        self.sendToNodes(MessageRep(**{
            f.MSG_TYPE.nm: msg_type,
            f.PARAMS.nm: msg.params,
            f.MSG.nm: resp
        }), names=[frm, ])

    def process_message_rep(self, msg: MessageRep, frm):
        msg_type = msg.msg_type
        if msg.msg is None:
            logger.debug('{} got null response for requested {} from {}'.
                         format(self, msg_type, frm))
            return
        return self.rep_handlers[msg_type](msg, frm)

    def valid_requested_msg(self, msg_type, **kwargs):
        return self.validation_handlers[msg_type](**kwargs)

    def request_msg(self, typ, params: Dict, frm: List[str]=None):
        self.sendToNodes(MessageReq(**{
            f.MSG_TYPE.nm: typ,
            f.PARAMS.nm: params
        }), names=frm)

    def _validate_requested_ledger_status(self, **kwargs):
        if kwargs['ledger_id'] in self.ledger_ids:
            if 'ledger_status' in kwargs:
                try:
                    # TODO: move this validation into MessageBase validation.
                    # TODO: avoid duplication of code here: create an instance of requested class in a one place (a factory?)
                    # depending on the msg_type

                    # the input is expected as a dict (serialization with ujson==1.33)
                    return LedgerStatus(**kwargs['ledger_status'])
                except TypeError as ex:
                    logger.warning(
                        '{} could not create LEDGER_STATUS out of {}'.
                            format(self, **kwargs['ledger_status']))
            else:
                return True

    def _serve_ledger_status_request(self, msg):
        params = msg.params
        ledger_id = params.get(f.LEDGER_ID.nm)
        if self.valid_requested_msg(msg.msg_type, ledger_id=ledger_id):
            return self.getLedgerStatus(ledger_id)
        else:
            self.discard(msg, 'cannot serve request',
                         logMethod=logger.debug)
            return False

    def _process_requested_ledger_status(self, msg, frm):
        params = msg.params
        ledger_id = params.get(f.LEDGER_ID.nm)
        ledger_status = msg.msg
        ledger_status = self.valid_requested_msg(msg.msg_type,
                                                 ledger_id=ledger_id,
                                                 ledger_status=ledger_status)
        if ledger_status:
            self.ledgerManager.processLedgerStatus(ledger_status, frm=frm)
            return
        self.discard(msg,
                     'cannot process requested message response',
                     logMethod=logger.debug)

    def _validate_requested_cons_proof(self, **kwargs):
        if kwargs['ledger_id'] in self.ledger_ids and \
                (isinstance(kwargs['seq_no_start'], int) and kwargs[
                    'seq_no_start'] > 0) and \
                (isinstance(kwargs['seq_no_end'], int) and kwargs[
                    'seq_no_end'] > 0):
            if 'cons_proof' in kwargs:
                try:
                    # the input is expected as a dict (serialization with ujson==1.33)
                    return ConsistencyProof(**kwargs['cons_proof'])
                except TypeError as ex:
                    logger.warning(
                        '{} could not create CONSISTENCY_PROOF out of {}'.
                            format(self, **kwargs['cons_proof']))
            else:
                return True

    def _serve_cons_proof_request(self, msg):
        params = msg.params
        ledger_id = params.get(f.LEDGER_ID.nm)
        seq_no_start = params.get(f.SEQ_NO_START.nm)
        seq_no_end = params.get(f.SEQ_NO_END.nm)
        if self.valid_requested_msg(msg.msg_type, ledger_id=ledger_id,
                                    seq_no_start=seq_no_start,
                                    seq_no_end=seq_no_end):
            return self.ledgerManager._buildConsistencyProof(ledger_id,
                                                             seq_no_start,
                                                             seq_no_end)
        else:
            self.discard(msg, 'cannot serve request',
                         logMethod=logger.debug)
            return False

    def _process_requested_cons_proof(self, msg, frm):
        params = msg.params
        ledger_id = params.get(f.LEDGER_ID.nm)
        seq_no_start = params.get(f.SEQ_NO_START.nm)
        seq_no_end = params.get(f.SEQ_NO_END.nm)
        cons_proof = msg.msg
        cons_proof = self.valid_requested_msg(msg.msg_type,
                                              ledger_id=ledger_id,
                                              seq_no_start=seq_no_start,
                                              seq_no_end=seq_no_end,
                                              cons_proof=cons_proof)
        if cons_proof:
            self.ledgerManager.processConsistencyProof(cons_proof, frm=frm)
            return
        self.discard(msg,
                     'cannot process requested message response',
                     logMethod=logger.debug)

    def _validate_requested_preprepare(self, **kwargs):
        if kwargs['inst_id'] in range(len(self.replicas)) and \
                        kwargs['view_no'] == self.viewNo and \
                isinstance(kwargs['pp_seq_no'], int) and \
                        kwargs['pp_seq_no'] > 0:
            if 'pp' in kwargs:
                try:
                    # the input is expected as a dict (serialization with ujson==1.33)
                    pp = PrePrepare(**kwargs['pp'])
                    if pp.instId != kwargs['inst_id'] or pp.viewNo != kwargs['view_no']:
                        logger.warning('{} found PREPREPARE {} not satisfying '
                                       'query criteria'.format(self, *kwargs['pp']))
                        return
                    return pp
                except TypeError as ex:
                    logger.warning(
                        '{} could not create PREPREPARE out of {}'.
                        format(self, **kwargs['pp']))
            else:
                return True

    def _serve_preprepare_request(self, msg):
        params = msg.params
        inst_id = params.get(f.INST_ID.nm)
        view_no = params.get(f.VIEW_NO.nm)
        pp_seq_no = params.get(f.PP_SEQ_NO.nm)
        if self.valid_requested_msg(msg.msg_type, inst_id=inst_id,
                                    view_no=view_no, pp_seq_no=pp_seq_no):
            return self.replicas[inst_id].getPrePrepare(view_no, pp_seq_no)
        else:
            self.discard(msg, 'cannot serve request',
                         logMethod=logger.debug)
            return False

    def _process_requested_preprepare(self, msg, frm):
        params = msg.params
        inst_id = params.get(f.INST_ID.nm)
        view_no = params.get(f.VIEW_NO.nm)
        pp_seq_no = params.get(f.PP_SEQ_NO.nm)
        pp = msg.msg
        pp = self.valid_requested_msg(msg.msg_type, inst_id=inst_id,
                                      view_no=view_no, pp_seq_no=pp_seq_no,
                                      pp=pp)
        if pp:
            frm = replica.Replica.generateName(frm, inst_id)
            self.replicas[inst_id].process_requested_pre_prepare(pp,
                                                                 sender=frm)
            return
        self.discard(msg,
                     'cannot process requested message response',
                     logMethod=logger.debug)

    def _validate_requested_propagate(self, **kwargs):
        if not (RequestIdentifierField().validate((kwargs['identifier'],
                                                   kwargs['req_id']))):
            if 'propagate' in kwargs:
                try:
                    # the input is expected as a dict (serialization with ujson==1.33)
                    ppg = Propagate(**kwargs['propagate'])
                    if ppg.request[f.IDENTIFIER.nm] != kwargs['identifier'] or \
                                    ppg.request[f.REQ_ID.nm] != kwargs['req_id']:
                            logger.warning('{} found PROPAGATE {} not '
                                           'satisfying query criteria'.format(self, *kwargs['ppg']))
                            return
                    return ppg
                except TypeError as ex:
                    logger.warning(
                        '{} could not create PROPAGATE out of {}'.
                            format(self, **kwargs['propagate']))
            else:
                return True

    def _serve_propagate_request(self, msg):
        params = msg.params
        identifier = params.get(f.IDENTIFIER.nm)
        req_id = params.get(f.REQ_ID.nm)
        if self.valid_requested_msg(msg.msg_type, identifier=identifier,
                                    req_id=req_id):
            req_key = (identifier, req_id)
            if req_key in self.requests and self.requests[req_key].finalised:
                sender_client = self.requestSender.get(req_key)
                req = self.requests[req_key].finalised
                return self.createPropagate(req, sender_client)
        else:
            self.discard(msg, 'cannot serve request',
                         logMethod=logger.debug)
            return False

    def _process_requested_propagate(self, msg, frm):
        params = msg.params
        identifier = params.get(f.IDENTIFIER.nm)
        req_id = params.get(f.REQ_ID.nm)
        ppg = msg.msg
        ppg = self.valid_requested_msg(msg.msg_type, identifier=identifier,
                                       req_id=req_id, propagate=ppg)
        if ppg:
            self.processPropagate(ppg, frm)
        else:
            self.discard(msg,
                         'cannot process requested message response',
                         logMethod=logger.debug)

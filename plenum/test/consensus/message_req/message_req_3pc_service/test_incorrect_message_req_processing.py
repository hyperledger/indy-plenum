from unittest.mock import Mock

import pytest

from plenum.common.constants import PREPREPARE, COMMIT, PREPARE, LEDGER_STATUS
from plenum.common.exceptions import IncorrectMessageForHandlingException
from plenum.common.messages.internal_messages import MissingMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep, PrePrepare, Prepare, Commit
from plenum.common.types import f
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root, logger


def raise_ex():
    raise IncorrectMessageForHandlingException(msg="", reason="", log_method=logger.info)


def test_process_message_req_incorrect_inst_id(message_req_service: MessageReqService, external_bus, data):
    key = (data.view_no, 1)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: PREPREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id + 1,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    message_req_service.process_message_req(message_req, "frm")
    assert len(external_bus.sent_messages) == 0


def test_process_message_req_handler_raise_ex(message_req_service: MessageReqService, external_bus, data):
    msg_type = PREPREPARE
    key = (data.view_no, 1)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: msg_type,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })

    message_req_service.handlers[msg_type].process_message_req = lambda msg: raise_ex()
    message_req_service.process_message_req(message_req, "frm")
    assert len(external_bus.sent_messages) == 0


def test_process_missing_message_incorrect_inst_id(message_req_service: MessageReqService, external_bus, data):
    frm = "frm"
    missing_msg = MissingMessage(msg_type=PREPREPARE,
                                 key=data.last_ordered_3pc,
                                 inst_id=data.inst_id + 1,
                                 dst=[frm],
                                 stash_data=None)
    message_req_service.process_missing_message(missing_msg)
    assert len(external_bus.sent_messages) == 0


def test_process_missing_message_raise_ex(message_req_service: MessageReqService, external_bus, data):
    frm = "frm"
    msg_type = PREPREPARE
    missing_msg = MissingMessage(msg_type=msg_type,
                                 key=data.last_ordered_3pc,
                                 inst_id=data.inst_id + 1,
                                 dst=[frm],
                                 stash_data=None)
    message_req_service.handlers[msg_type].prepare_msg_to_request = lambda msg: raise_ex()
    message_req_service.process_missing_message(missing_msg)
    assert len(external_bus.sent_messages) == 0


def test_process_message_rep_without_msg(message_req_service: MessageReqService, external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    message_req_service.handlers[PREPREPARE].requested_messages[key] = None
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: PREPREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: None
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(PrePrepare, network_handler)
    message_req_service.process_message_rep(message_rep, frm)
    network_handler.assert_not_called()


def test_process_message_rep_invalid_preprepare(message_req_service: MessageReqService, external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    msg_type = PREPREPARE
    message_req_service.handlers[PREPREPARE].requested_messages[key] = None
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: msg_type,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: dict(pp.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(PrePrepare, network_handler)
    message_req_service.handlers[msg_type].extract_message = lambda msg, frm: raise_ex()
    message_req_service.process_message_rep(message_rep, frm)
    network_handler.assert_not_called()


def test_process_message_rep_invalid_inst_id(message_req_service: MessageReqService, external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    msg_type = PREPREPARE
    message_req_service.handlers[PREPREPARE].requested_messages[key] = None
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: msg_type,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id + 1,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: dict(pp.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(PrePrepare, network_handler)
    message_req_service.process_message_rep(message_rep, frm)
    network_handler.assert_not_called()


def test_process_message_rep_with_incorrect_type(message_req_service: MessageReqService, external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: LEDGER_STATUS,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: dict(pp.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(PrePrepare, network_handler)
    external_bus.subscribe(Prepare, network_handler)
    external_bus.subscribe(Commit, network_handler)
    message_req_service.process_message_rep(message_rep, frm)
    network_handler.assert_not_called()


def test_process_message_req_with_incorrect_type(message_req_service: MessageReqService, external_bus, data, pp):
    msg_type = LEDGER_STATUS
    key = (data.view_no, 1)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: msg_type,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    message_req_service.process_message_req(message_req, "frm")
    assert len(external_bus.sent_messages) == 0



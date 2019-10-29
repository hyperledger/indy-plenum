from unittest.mock import Mock

import pytest

from plenum.common.constants import PREPREPARE, PREPARE
from plenum.common.messages.internal_messages import MissingMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep, PrePrepare, Prepare
from plenum.common.types import f
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root, create_prepare


def test_process_message_req_prepare(message_req_service: MessageReqService, external_bus, data, prepare):
    key = (prepare.viewNo, prepare.ppSeqNo)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: PREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    frm = "frm"
    message_req_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                        message_req.params,
                                                        data.prepares[key].msg),
                                             [frm])


def test_process_message_req_prepare_without_prepare(message_req_service: MessageReqService,
                                                     external_bus, data, prepare):
    key = (prepare.viewNo, prepare.ppSeqNo)
    other_node_name = "other_node"
    data.prepares.clear()
    data.prepares.addVote(prepare, other_node_name)
    assert not data.prepares.hasPrepareFrom(prepare, data.name)
    assert data.prepares.hasPrepareFrom(prepare, other_node_name)

    message_req = MessageReq(**{
        f.MSG_TYPE.nm: PREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    frm = "frm"
    message_req_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 0


def test_process_missing_message_prepare(message_req_service: MessageReqService, external_bus, data):
    frm = "frm"
    view_no = data.view_no
    pp_seq_no = data.last_ordered_3pc[1] + 1
    missing_msg = MissingMessage(msg_type=PREPARE,
                                 key=(view_no, pp_seq_no),
                                 inst_id=data.inst_id,
                                 dst=[frm],
                                 stash_data=None)
    message_req_service.process_missing_message(missing_msg)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageReq(PREPARE,
                                                        {f.INST_ID.nm: data.inst_id,
                                                         f.VIEW_NO.nm: view_no,
                                                         f.PP_SEQ_NO.nm: pp_seq_no}),
                                             [frm])


def test_process_message_prepare(message_req_service: MessageReqService, external_bus, data, prepare):
    key = (prepare.viewNo, prepare.ppSeqNo)
    message_req_service.handlers[PREPARE].requested_messages[key] = None
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: PREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: dict(prepare.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(Prepare, network_handler)
    message_req_service.process_message_rep(message_rep, frm)
    network_handler.assert_called_once_with(prepare, frm)

from unittest.mock import Mock

import pytest

from plenum.common.constants import PREPREPARE
from plenum.common.messages.internal_messages import Missing3pcMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep, PrePrepare
from plenum.common.types import f
from plenum.server.consensus.message_request.message_req_3pc_service import MessageReq3pcService
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root


@pytest.fixture
def data(consensus_data):
    return consensus_data("MessageReq3pcService")


@pytest.fixture
def pp(data):
    pp = create_pre_prepare_no_bls(generate_state_root(),
                                   pp_seq_no=1,
                                   view_no=data.view_no)
    data.sent_preprepares[(pp.viewNo, pp.ppSeqNo)] = pp
    return pp


@pytest.fixture
def message_req_3pc_service(data, internal_bus, external_bus):
    req_service = MessageReq3pcService(data=data,
                                       bus=internal_bus,
                                       network=external_bus)
    return req_service


def test_process_message_req(message_req_3pc_service: MessageReq3pcService, external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: PREPREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    frm = "frm"
    message_req_3pc_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                        message_req.params,
                                                        data.sent_preprepares[key]),
                                             [frm])


def test_process_missing_message(message_req_3pc_service: MessageReq3pcService, external_bus, data):
    frm = "frm"
    missing_msg = Missing3pcMessage(msg_type=TestHandler.MSG_TYPE,
                                    three_pc_key=data.last_ordered_3pc,
                                    inst_id=data.inst_id,
                                    dst=[frm],
                                    stash_data=None)
    message_req_3pc_service.process_missing_message(missing_msg)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageReq(TestHandler.MSG_TYPE,
                                                        TestHandler.PREPARE_PARAMS),
                                             [frm])


def test_process_message_rep(message_req_3pc_service: MessageReq3pcService, external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: PREPREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: pp
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(PrePrepare, network_handler)
    message_req_3pc_service.process_message_rep(message_rep, frm)
    network_handler.assert_called_once_with((pp, frm))

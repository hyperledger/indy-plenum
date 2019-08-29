from typing import Dict, Any, Optional, Tuple, NamedTuple
from unittest.mock import Mock

import pytest

from plenum.common.messages.internal_messages import Missing3pcMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep
from plenum.common.types import f
from plenum.server.consensus.message_request.message_req_3pc_service import MessageReq3pcService
from plenum.server.message_handlers import ThreePhaseMessagesHandler


class TestHandler(ThreePhaseMessagesHandler):
    MSG_TYPE = "TEST_TYPE"
    RESPONSE = "response"
    PREPARE_PARAMS = {"test": 1}
    msg_cls = NamedTuple('Mock3pcMessage', [])

    def validate(self, **kwargs) -> bool:
        return True

    def create(self, msg: Dict, **kwargs) -> Any:
        return self.msg_cls()

    def requestor(self, params: Dict[str, Any]) -> Any:
        return self.RESPONSE

    def processor(self, validated_msg: object, params: Dict[str, Any], frm: str) -> Optional:
        pass

    def prepare_msg_to_request(self, three_pc_key: Tuple[int, int],
                               stash_data: Optional[Tuple[str, str, str]] = None) -> Optional[Dict]:
        return self.PREPARE_PARAMS


@pytest.fixture
def data(consensus_data):
    return consensus_data("MessageReq3pcService")

@pytest.fixture
def handler(data, internal_bus, external_bus):
    return TestHandler(data)


@pytest.fixture
def message_req_3pc_service(data, internal_bus, external_bus, handler):
    req_service = MessageReq3pcService(data=data,
                                       bus=internal_bus,
                                       network=external_bus)
    MessageReq.allowed_types.add(TestHandler.MSG_TYPE)
    req_service.handlers[TestHandler.MSG_TYPE] = handler
    return req_service


def test_process_message_req(message_req_3pc_service: MessageReq3pcService, external_bus):
    message_req = MessageReq(TestHandler.MSG_TYPE, {})
    frm = "frm"
    message_req_3pc_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageRep(TestHandler.MSG_TYPE,
                                                        message_req.params,
                                                        TestHandler.RESPONSE),
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


def test_process_message_rep(message_req_3pc_service: MessageReq3pcService, external_bus, handler, data):
    params = {f.INST_ID.nm: data.inst_id,
              f.VIEW_NO.nm: data.view_no,
              f.PP_SEQ_NO.nm: 1,
              "msg": "msg"}
    message_rep = MessageRep(TestHandler.MSG_TYPE, params, {"msg": 1})
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(handler.msg_cls, network_handler)
    message_req_3pc_service.process_message_rep(message_rep, frm)
    network_handler.assert_called_once_with((handler.msg_cls(), frm))

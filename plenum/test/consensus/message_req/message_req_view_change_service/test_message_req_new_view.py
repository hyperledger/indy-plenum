from unittest.mock import Mock

import pytest

from plenum.common.constants import COMMIT, NEW_VIEW
from plenum.common.messages.internal_messages import MissingMessage, ViewChangeStarted
from plenum.common.messages.node_messages import MessageReq, MessageRep, Commit, ViewChange, ViewChangeAck, NewView
from plenum.common.types import f
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.consensus.view_change_storages import view_change_digest
from plenum.test.consensus.helper import create_view_change, create_new_view
from plenum.test.helper import create_commit_no_bls_sig

msg_count = 5


@pytest.fixture()
def message_req_service(message_req_service):
    # view_no can't be 0 in view change
    message_req_service._data.view_no += 1
    return message_req_service


@pytest.fixture()
def fill_requested_lists(message_req_service):
    for handler in message_req_service.handlers.values():
        for seq_no in range(msg_count):
            handler.requested_messages[(0, seq_no)] = None


@pytest.fixture()
def new_view_message(data):
    return create_new_view(data.view_no - 1, 5)


def test_process_message_req_new_view(message_req_service: MessageReqService,
                                      external_bus, data: ConsensusSharedData,
                                      new_view_message: NewView):
    frm = "frm"
    data.primary_name = data.name
    data.new_view_votes.add_new_view(new_view_message, data.primary_name)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: NEW_VIEW,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: data.view_no},
    })
    external_bus.process_incoming(message_req, frm)
    assert len(external_bus.sent_messages) == 1

    expected_new_view = new_view_message._asdict()
    expected_new_view.update({f.PRIMARY.nm: replica_name_to_node_name(data.primary_name)})

    assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                        message_req.params,
                                                        expected_new_view),
                                             [frm])


def test_process_message_req_new_view_by_non_primary(message_req_service: MessageReqService,
                                                     external_bus, data: ConsensusSharedData,
                                                     new_view_message: NewView):
    frm = "frm"
    data.primary_name = "a" + data.name
    data.new_view_votes.add_new_view(new_view_message, data.primary_name)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: NEW_VIEW,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: data.view_no},
    })
    external_bus.process_incoming(message_req, frm)
    assert len(external_bus.sent_messages) == 1


def test_process_missing_message_new_view(message_req_service: MessageReqService, external_bus, data,
                                          internal_bus, new_view_message: NewView):
    primary = data.primary_name
    inst_id = data.inst_id
    missing_msg = MissingMessage(msg_type=NEW_VIEW,
                                 key=data.view_no,
                                 inst_id=inst_id,
                                 dst=[primary],
                                 stash_data=None)
    internal_bus.send(missing_msg)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageReq(NEW_VIEW,
                                                        {f.INST_ID.nm: inst_id,
                                                         f.VIEW_NO.nm: data.view_no}),
                                             [primary])


def test_process_message_rep_new_view_from_primary(message_req_service: MessageReqService, external_bus, data,
                                                   new_view_message: NewView):
    primary = data.primary_name
    inst_id = data.inst_id
    message_req_service.handlers[NEW_VIEW].requested_messages[data.view_no] = None
    message_rep_from_primary = MessageRep(**{
        f.MSG_TYPE.nm: NEW_VIEW,
        f.PARAMS.nm: {f.INST_ID.nm: inst_id,
                      f.VIEW_NO.nm: data.view_no},
        f.MSG.nm: dict(new_view_message.items())
    })
    network_handler = Mock()
    external_bus.subscribe(NewView, network_handler)
    message_req_service.process_message_rep(message_rep_from_primary, primary)
    network_handler.assert_called_once_with(new_view_message, primary)


def test_process_message_rep_incorrect_new_view(message_req_service: MessageReqService,
                                                external_bus, data,
                                                new_view_message: NewView):
    frm = data.primary_name
    inst_id = data.inst_id
    message_req_service.handlers[NEW_VIEW].requested_messages[data.view_no] = None
    message_rep_from_primary = MessageRep(**{
        f.MSG_TYPE.nm: NEW_VIEW,
        f.PARAMS.nm: {f.INST_ID.nm: inst_id,
                      f.VIEW_NO.nm: data.view_no},
        f.MSG.nm: dict(new_view_message.items())
    })
    message_rep_from_primary.msg[f.VIEW_NO.nm] += 1
    network_handler = Mock()
    external_bus.subscribe(NewView, network_handler)
    message_req_service.process_message_rep(message_rep_from_primary, frm)
    network_handler.assert_not_called()


def test_process_view_change_started(message_req_service: MessageReqService, internal_bus, data,
                                     fill_requested_lists):
    handler = message_req_service.handlers[NEW_VIEW]
    msg = ViewChangeStarted(0)
    internal_bus.send(msg)
    assert not handler.requested_messages

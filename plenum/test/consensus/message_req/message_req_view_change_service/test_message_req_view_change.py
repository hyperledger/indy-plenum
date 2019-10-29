from unittest.mock import Mock

import pytest

from plenum.common.constants import COMMIT, VIEW_CHANGE
from plenum.common.messages.internal_messages import MissingMessage, ViewChangeStarted
from plenum.common.messages.node_messages import MessageReq, MessageRep, Commit, ViewChange, ViewChangeAck
from plenum.common.types import f
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.server.consensus.view_change_storages import view_change_digest
from plenum.test.consensus.helper import create_view_change
from plenum.test.helper import create_commit_no_bls_sig

msg_count = 5


@pytest.fixture()
def fill_requested_lists(message_req_service):
    for handler in message_req_service.handlers.values():
        for seq_no in range(msg_count):
            handler.requested_messages[(0, seq_no)] = None


@pytest.fixture()
def view_change_message(data):
    return create_view_change(data.view_no)


def test_process_message_req_view_change(message_req_service: MessageReqService,
                                         external_bus, data: ConsensusSharedData,
                                         view_change_message: ViewChange):
    frm = "frm"
    digest = view_change_digest(view_change_message)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: VIEW_CHANGE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.DIGEST.nm: digest,
                      f.NAME.nm: frm},
    })
    data.view_change_votes.add_view_change(view_change_message, frm)
    external_bus.process_incoming(message_req, frm)
    assert len(external_bus.sent_messages) == 1

    assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                        message_req.params,
                                                        view_change_message._asdict()),
                                             [frm])


def test_process_missing_message_view_change(message_req_service: MessageReqService, external_bus, data,
                                             internal_bus, view_change_message: ViewChange):
    frm = "frm"
    confused_node = "confused_node"
    inst_id = data.inst_id
    digest = view_change_digest(view_change_message)
    missing_msg = MissingMessage(msg_type=VIEW_CHANGE,
                                 key=(frm, digest),
                                 inst_id=inst_id,
                                 dst=[confused_node],
                                 stash_data=None)
    internal_bus.send(missing_msg)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageReq(VIEW_CHANGE,
                                                        {f.INST_ID.nm: inst_id,
                                                         f.DIGEST.nm: digest,
                                                         f.NAME.nm: frm}),
                                             [confused_node])


def test_process_message_rep_view_change_by_quorum(message_req_service: MessageReqService, external_bus, data,
                                                   view_change_message: ViewChange):
    frm = "frm"
    inst_id = data.inst_id
    digest = view_change_digest(view_change_message)
    key = (frm, digest)
    message_req_service.handlers[VIEW_CHANGE].requested_messages[key] = None
    message_rep_from_primary = MessageRep(**{
        f.MSG_TYPE.nm: VIEW_CHANGE,
        f.PARAMS.nm: {f.INST_ID.nm: inst_id,
                      f.DIGEST.nm: digest,
                      f.NAME.nm: frm},
        f.MSG.nm: dict(view_change_message.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(ViewChange, network_handler)
    for i in range(data.quorums.weak.value):
        network_handler.assert_not_called()
        message_req_service.process_message_rep(message_rep_from_primary, str(i))
    network_handler.assert_called_once_with(view_change_message, frm)


def test_process_message_rep_view_change_from_one(message_req_service: MessageReqService, external_bus, data,
                                                  view_change_message: ViewChange):
    frm = "frm"
    inst_id = data.inst_id
    digest = view_change_digest(view_change_message)
    key = (frm, digest)
    message_req_service.handlers[VIEW_CHANGE].requested_messages[key] = None
    message_rep_from_primary = MessageRep(**{
        f.MSG_TYPE.nm: VIEW_CHANGE,
        f.PARAMS.nm: {f.INST_ID.nm: inst_id,
                      f.DIGEST.nm: digest,
                      f.NAME.nm: frm},
        f.MSG.nm: dict(view_change_message.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(ViewChange, network_handler)
    network_handler.assert_not_called()
    message_req_service.process_message_rep(message_rep_from_primary, frm)
    network_handler.assert_called_once_with(view_change_message, frm)


def test_process_view_change_started(message_req_service: MessageReqService, internal_bus, data,
                                     fill_requested_lists):
    handler = message_req_service.handlers[VIEW_CHANGE]
    handler._received_vc[("NodeName", "digest")] = {"Node1", "Node2"}
    msg = ViewChangeStarted(0)
    internal_bus.send(msg)
    assert not handler.requested_messages
    assert not handler._received_vc

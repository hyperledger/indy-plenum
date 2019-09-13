from unittest.mock import Mock

import pytest

from plenum.common.constants import COMMIT, VIEW_CHANGE
from plenum.common.messages.internal_messages import MissingMessage, MissingVCMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep, Commit, ViewChange, ViewChangeAck
from plenum.common.types import f
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.message_request.message_req_3pc_service import MessageReq3pcService
from plenum.server.consensus.view_change_service import view_change_digest
from plenum.test.helper import create_commit_no_bls_sig


def test_process_message_req_view_change(message_req_3pc_service: MessageReq3pcService,
                                         external_bus, data: ConsensusSharedData,
                                         view_change_message: ViewChange):
    frm = "frm"
    view_no = data.view_no
    digest = view_change_digest(view_change_message)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: VIEW_CHANGE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: view_no,
                      f.DIGEST.nm: digest,
                      f.NAME.nm: frm},
    })
    external_bus.process_incoming(message_req, frm)
    assert len(external_bus.sent_messages) == 1

    assert data.is_primary is not None
    if data.is_primary:
        assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                            message_req.params,
                                                            view_change_message),
                                                 [frm])
    else:
        assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                            message_req.params,
                                                            ViewChangeAck(view_no,
                                                                          frm,
                                                                          digest)),
                                                 [frm])


def test_process_missing_message_view_change(message_req_3pc_service: MessageReq3pcService, external_bus, data,
                                             view_change_message: ViewChange):
    frm = "frm"
    confused_node = "confused_node"
    view_no = view_change_message.viewNo
    inst_id = data.inst_id
    digest = view_change_digest(view_change_message)
    missing_msg = MissingVCMessage(msg_type=VIEW_CHANGE,
                                   view_no=view_no,
                                   inst_id=inst_id,
                                   name=frm,
                                   digest=digest)
    external_bus.process_incoming(missing_msg, confused_node)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageReq(VIEW_CHANGE,
                                                        {f.INST_ID.nm: inst_id,
                                                         f.DIGEST.nm: digest,
                                                         f.NAME.nm: frm}),
                                             [confused_node])


def test_process_message_rep_view_change(message_req_3pc_service: MessageReq3pcService, external_bus, data,
                                         view_change_message: ViewChange):
    frm = "frm"
    confused_node = "confused_node"
    view_no = view_change_message.viewNo
    inst_id = data.inst_id
    digest = view_change_digest(view_change_message)
    message_req_3pc_service.handlers[VIEW_CHANGE].requested_messages[key] = None
    message_rep_from_primary = MessageRep(**{
        f.MSG_TYPE.nm: VIEW_CHANGE,
        f.PARAMS.nm: {f.INST_ID.nm: inst_id,
                      f.VIEW_NO.nm: view_no,
                      f.DIGEST.nm: digest,
                      f.NAME.nm: frm},
        f.MSG.nm: dict(view_change_message.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(Commit, network_handler)
    message_req_3pc_service.process_message_rep(message_rep, frm)
    network_handler.assert_called_once_with(commit, frm)

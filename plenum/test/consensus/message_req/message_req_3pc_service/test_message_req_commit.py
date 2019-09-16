from unittest.mock import Mock

import pytest

from plenum.common.constants import COMMIT
from plenum.common.messages.internal_messages import MissingMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep, Commit
from plenum.common.types import f
from plenum.server.consensus.message_request.message_req_3pc_service import MessageReq3pcService
from plenum.test.helper import create_commit_no_bls_sig


def test_process_message_req_commit(message_req_3pc_service: MessageReq3pcService, external_bus, data, commit):
    key = (commit.viewNo, commit.ppSeqNo)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: COMMIT,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    frm = "frm"
    message_req_3pc_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                        message_req.params,
                                                        data.commits[key].msg),
                                             [frm])


def test_process_missing_message_commit(message_req_3pc_service: MessageReq3pcService, external_bus, data):
    frm = "frm"
    missing_msg = MissingMessage(msg_type=COMMIT,
                                 key=data.last_ordered_3pc,
                                 inst_id=data.inst_id,
                                 dst=[frm],
                                 stash_data=None)
    message_req_3pc_service.process_missing_message(missing_msg)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageReq(COMMIT,
                                                        {f.INST_ID.nm: data.inst_id,
                                                         f.VIEW_NO.nm: data.last_ordered_3pc[0],
                                                         f.PP_SEQ_NO.nm: data.last_ordered_3pc[1]}),
                                             [frm])


def test_process_message_rep_commit(message_req_3pc_service: MessageReq3pcService, external_bus, data, commit):
    key = (commit.viewNo, commit.ppSeqNo)
    message_req_3pc_service.handlers[COMMIT].requested_messages[key] = None
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: COMMIT,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: dict(commit.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(Commit, network_handler)
    message_req_3pc_service.process_message_rep(message_rep, frm)
    network_handler.assert_called_once_with(commit, frm)

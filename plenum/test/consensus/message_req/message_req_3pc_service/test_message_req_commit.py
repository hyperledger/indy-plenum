from unittest.mock import Mock

import pytest

from plenum.common.constants import COMMIT
from plenum.common.messages.internal_messages import MissingMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep, Commit
from plenum.common.types import f
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.test.helper import create_commit_no_bls_sig


def test_process_message_req_commit(message_req_service: MessageReqService, external_bus, data, commit):
    key = (commit.viewNo, commit.ppSeqNo)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: COMMIT,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    frm = "frm"
    message_req_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                        message_req.params,
                                                        data.commits[key].msg),
                                             [frm])


def test_process_message_req_commit_without_commit(message_req_service: MessageReqService, external_bus, data, commit):
    key = (commit.viewNo, commit.ppSeqNo)
    other_node_name = "other_node"
    data.commits.clear()
    data.commits.addVote(commit, other_node_name)
    assert not data.commits.hasCommitFrom(commit, data.name)
    assert data.commits.hasCommitFrom(commit, other_node_name)

    message_req = MessageReq(**{
        f.MSG_TYPE.nm: COMMIT,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    frm = "frm"
    message_req_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 0


def test_process_missing_message_commit(message_req_service: MessageReqService, external_bus, data):
    frm = "frm"
    view_no = data.view_no
    pp_seq_no = data.last_ordered_3pc[1] + 1
    missing_msg = MissingMessage(msg_type=COMMIT,
                                    key=(view_no, pp_seq_no),
                                    inst_id=data.inst_id,
                                    dst=[frm],
                                    stash_data=None)
    message_req_service.process_missing_message(missing_msg)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageReq(COMMIT,
                                                        {f.INST_ID.nm: data.inst_id,
                                                         f.VIEW_NO.nm: view_no,
                                                         f.PP_SEQ_NO.nm: pp_seq_no}),
                                             [frm])


def test_process_message_rep_commit(message_req_service: MessageReqService, external_bus, data, commit):
    key = (commit.viewNo, commit.ppSeqNo)
    message_req_service.handlers[COMMIT].requested_messages[key] = None
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
    message_req_service.process_message_rep(message_rep, frm)
    network_handler.assert_called_once_with(commit, frm)

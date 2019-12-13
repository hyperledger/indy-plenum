from unittest.mock import Mock

import pytest

from plenum.common.constants import PREPREPARE, COMMIT, PREPARE
from plenum.common.exceptions import IncorrectMessageForHandlingException
from plenum.common.messages.internal_messages import MissingMessage, ViewChangeStarted, CheckpointStabilized
from plenum.common.messages.node_messages import MessageReq, MessageRep, PrePrepare, Ordered
from plenum.common.types import f
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root, logger

msg_count = 5


@pytest.fixture()
def fill_requested_lists(message_req_service):
    for handler in message_req_service.handlers.values():
        for seq_no in range(msg_count):
            handler.requested_messages[(0, seq_no)] = None


def test_process_checkpoint_stabilized(message_req_service: MessageReqService, internal_bus, data,
                                       fill_requested_lists):
    pp_seq_no = msg_count // 2
    view_no = 0
    msg = CheckpointStabilized(last_stable_3pc=(view_no, pp_seq_no))
    internal_bus.send(msg)
    for key, handler in message_req_service.handlers.items():
        if key not in message_req_service.three_pc_handlers:
            continue
        for requested_pp_seq_no in range(msg_count):
            if requested_pp_seq_no <= pp_seq_no:
                assert (view_no, requested_pp_seq_no) not in handler.requested_messages
            else:
                assert (view_no, requested_pp_seq_no) in handler.requested_messages


def test_process_view_change_started(message_req_service: MessageReqService, internal_bus, data,
                                     fill_requested_lists):
    msg = ViewChangeStarted(0)
    internal_bus.send(msg)
    for handler in message_req_service.handlers.values():
        assert not handler.requested_messages


def test_process_ordered(message_req_service: MessageReqService, internal_bus, data, pp, fill_requested_lists):
    pp_seq_no = msg_count // 2
    view_no = 0
    ord_args = [
        pp.instId,
        view_no,
        pp.reqIdr,
        [],
        pp_seq_no,
        pp.ppTime,
        pp.ledgerId,
        pp.stateRootHash,
        pp.txnRootHash,
        pp.auditTxnRootHash,
        ["Alpha", "Beta"],
        ["Alpha", "Beta", "Gamma", "Delta"],
        view_no,
        'digest'
    ]
    msg = Ordered(*ord_args)
    internal_bus.send(msg)
    for key, handler in message_req_service.handlers.items():
        if key not in message_req_service.three_pc_handlers:
            continue
        for requested_pp_seq_no in range(msg_count):
            if requested_pp_seq_no == pp_seq_no:
                assert (view_no, pp_seq_no) not in handler.requested_messages
            else:
                assert (view_no, requested_pp_seq_no) in handler.requested_messages

from unittest.mock import Mock

from plenum.common.constants import PREPREPARE
from plenum.common.messages.internal_messages import MissingMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep, PrePrepare
from plenum.common.types import f
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.server.consensus.utils import preprepare_to_batch_id


def test_process_message_req_preprepare(message_req_service: MessageReqService, external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: PREPREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    frm = "frm"
    message_req_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageRep(message_req.msg_type,
                                                        message_req.params,
                                                        data.sent_preprepares[key]),
                                             [frm])


def test_process_message_req_preprepare_without_preprepare(message_req_service: MessageReqService,
                                                           external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    data.sent_preprepares.pop(key)
    message_req = MessageReq(**{
        f.MSG_TYPE.nm: PREPREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
    })
    frm = "frm"
    message_req_service.process_message_req(message_req, frm)
    assert len(external_bus.sent_messages) == 0


def test_process_missing_message_preprepare(message_req_service: MessageReqService, external_bus, data):
    frm = "frm"
    view_no = data.view_no
    pp_seq_no = data.last_ordered_3pc[1] + 1
    missing_msg = MissingMessage(msg_type=PREPREPARE,
                                 key=(view_no, pp_seq_no),
                                 inst_id=data.inst_id,
                                 dst=[frm],
                                 stash_data=None)
    message_req_service.process_missing_message(missing_msg)
    assert len(external_bus.sent_messages) == 1
    assert external_bus.sent_messages[0] == (MessageReq(PREPREPARE,
                                                        {f.INST_ID.nm: data.inst_id,
                                                         f.VIEW_NO.nm: view_no,
                                                         f.PP_SEQ_NO.nm: pp_seq_no}),
                                             [frm])


def test_process_message_rep_preprepare(message_req_service: MessageReqService, external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    message_req_service.handlers[PREPREPARE].requested_messages[key] = None
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: PREPREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: dict(pp.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(PrePrepare, network_handler)
    message_req_service.process_message_rep(message_rep, frm)
    network_handler.assert_called_once_with(pp, frm)


def test_process_message_rep_already_ordered_preprepare(message_req_service: MessageReqService,
                                                        external_bus, data, pp):
    key = (pp.viewNo, pp.ppSeqNo)
    data.preprepared.append(preprepare_to_batch_id(pp))
    message_req_service.handlers[PREPREPARE].requested_messages[key] = None
    message_rep = MessageRep(**{
        f.MSG_TYPE.nm: PREPREPARE,
        f.PARAMS.nm: {f.INST_ID.nm: data.inst_id,
                      f.VIEW_NO.nm: key[0],
                      f.PP_SEQ_NO.nm: key[1]},
        f.MSG.nm: dict(pp.items())
    })
    frm = "frm"
    network_handler = Mock()
    external_bus.subscribe(PrePrepare, network_handler)
    message_req_service.process_message_rep(message_rep, frm)
    network_handler.assert_not_called()

from plenum.common.messages.node_messages import Prepare, OldViewPrePrepareRequest, OldViewPrePrepareReply
from plenum.common.util import get_utc_epoch


def expect_suspicious(orderer, suspicious_code):
    def reportSuspiciousNodeEx(ex):
        assert suspicious_code == ex.code
        raise ex

    orderer.report_suspicious_node = reportSuspiciousNodeEx


def _register_pp_ts(orderer, pp, sender):
    tpcKey = (pp.viewNo, pp.ppSeqNo)
    ppKey = (pp.auditTxnRootHash, sender)
    orderer.pre_prepare_tss[tpcKey][ppKey] = get_utc_epoch()


def check_suspicious(handler, ex_message):
    suspicious = handler.call_args_list
    if len(suspicious) > 1:
        print(suspicious)
    assert len(suspicious) == 1
    assert suspicious[0][0][0].inst_id == ex_message.inst_id
    ex = suspicious[0][0][0].ex
    assert ex.code == ex_message.ex.code
    assert ex.offendingMsg == ex_message.ex.offendingMsg
    assert ex.node == ex_message.ex.node


def check_prepares_sent(external_bus, pre_prepares, view_no):
    assert len(external_bus.sent_messages) >= len(pre_prepares)
    for i, pre_prepare in enumerate(pre_prepares):
        msg, dst = external_bus.sent_messages[i]
        assert dst is None  # message was broadcast
        assert isinstance(msg, Prepare)
        assert msg.instId == pre_prepare.instId
        assert msg.viewNo == view_no
        assert msg.ppSeqNo == pre_prepare.ppSeqNo
        assert msg.ppTime == pre_prepare.ppTime
        assert msg.digest == pre_prepare.digest
        assert msg.stateRootHash == pre_prepare.stateRootHash
        assert msg.txnRootHash == pre_prepare.txnRootHash
        assert msg.auditTxnRootHash == pre_prepare.auditTxnRootHash


def check_request_old_view_preprepares_sent(external_bus, batches):
    if not batches and not external_bus.sent_messages:
        return

    msg, dst = external_bus.sent_messages[-1]

    if not batches:
        assert not isinstance(msg, OldViewPrePrepareRequest)
        return

    assert dst is None  # message was broadcast
    assert isinstance(msg, OldViewPrePrepareRequest)
    assert msg.instId == 0
    assert msg.batch_ids == batches


def check_reply_old_view_preprepares_sent(external_bus, frm, pre_prepares):
    assert len(external_bus.sent_messages) >= 0
    msg, dst = external_bus.sent_messages[-1]
    assert dst == [frm]
    assert isinstance(msg, OldViewPrePrepareReply)
    assert msg.instId == 0
    assert msg.preprepares == pre_prepares, "{} != {}".format(msg.preprepares, pre_prepares)

from plenum.common.util import get_utc_epoch


def expect_suspicious(orderer, suspicious_code):
    def reportSuspiciousNodeEx(ex):
        assert suspicious_code == ex.code
        raise ex

    orderer.report_suspicious_node = reportSuspiciousNodeEx


def _register_pp_ts(orderer, pp, sender):
    tpcKey = (pp.viewNo, pp.ppSeqNo)
    ppKey = (pp, sender)
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

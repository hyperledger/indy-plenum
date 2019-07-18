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

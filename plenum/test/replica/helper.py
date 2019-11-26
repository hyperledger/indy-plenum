from plenum.common.messages.internal_messages import CatchupFinished


def emulate_catchup(replica, ppSeqNo=100):
    replica.internal_bus.send(CatchupFinished((replica.viewNo, ppSeqNo),
                                              replica.last_ordered_3pc))


def emulate_select_primaries(replica):
    replica.primaryName = 'SomeAnotherNode'
    replica._ordering_service._setup_for_non_master_after_view_change(replica.viewNo)


def expect_suspicious(replica, suspicious_code):
    def reportSuspiciousNodeEx(ex):
        assert suspicious_code == ex.code
        raise ex

    replica.node.reportSuspiciousNodeEx = reportSuspiciousNodeEx

def register_pp_ts(replica, pp, sender):
    tpcKey = (pp.viewNo, pp.ppSeqNo)
    ppKey = (pp.auditTxnRootHash, sender)
    replica._ordering_service.pre_prepare_tss[tpcKey][ppKey] = replica.get_time_for_3pc_batch()

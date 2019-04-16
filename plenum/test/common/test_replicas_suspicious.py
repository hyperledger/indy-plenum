from plenum.common.constants import DOMAIN_LEDGER_ID

from plenum.test.helper import waitForViewChange
from stp_core.loop.eventually import eventually


def test_nodes_make_view_change_only_on_master_suspicious(
        looper, txnPoolNodeSet):
    old_view = txnPoolNodeSet[0].viewNo

    primary1 = txnPoolNodeSet[0].replicas[0]
    primary2 = txnPoolNodeSet[1].replicas[1]
    assert primary1.isPrimary is True
    assert primary2.isPrimary is True

    primary1.batchDigest = lambda reqs: 'asd'
    primary2.batchDigest = lambda reqs: 'asd'

    non_primary1 = txnPoolNodeSet[0].replicas[1]
    old_pp = non_primary1.spylog.count(non_primary1.processPrePrepare)

    def pp_processed(replica, old_pp):
        assert replica.spylog.count(replica.processPrePrepare) == old_pp + 1

    primary2._do_send_3pc_batch(DOMAIN_LEDGER_ID)
    looper.run(eventually(pp_processed, non_primary1, old_pp))
    waitForViewChange(looper, txnPoolNodeSet, old_view)

    non_primary2 = txnPoolNodeSet[1].replicas[0]
    old_pp = non_primary2.spylog.count(non_primary2.processPrePrepare)

    primary1._do_send_3pc_batch(DOMAIN_LEDGER_ID)
    looper.run(eventually(pp_processed, non_primary2, old_pp))
    waitForViewChange(looper, txnPoolNodeSet, old_view + 1)

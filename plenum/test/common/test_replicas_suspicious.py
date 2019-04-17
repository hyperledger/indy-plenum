from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.server.view_change.view_changer import ViewChanger

from plenum.test.helper import waitForViewChange
from stp_core.loop.eventually import eventually


def test_nodes_make_view_change_only_on_master_suspicious(
        looper, txnPoolNodeSet):
    old_view = txnPoolNodeSet[0].viewNo

    master_primary = txnPoolNodeSet[0].replicas[0]
    bacup_primary = txnPoolNodeSet[1].replicas[1]
    assert master_primary.isPrimary is True
    assert bacup_primary.isPrimary is True

    master_primary.batchDigest = lambda reqs: 'asd'
    bacup_primary.batchDigest = lambda reqs: 'asd'

    non_primary_backup = txnPoolNodeSet[0].replicas[1]
    old_pp = non_primary_backup.spylog.count(non_primary_backup.processPrePrepare)

    def pp_processed(replica, old_pp):
        assert replica.spylog.count(replica.processPrePrepare) == old_pp + 1

    bacup_primary._do_send_3pc_batch(DOMAIN_LEDGER_ID)
    looper.run(eventually(pp_processed, non_primary_backup, old_pp))
    assert all(node.view_changer.spylog.count(ViewChanger.sendInstanceChange) == 0
               for node in txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, old_view)

    non_primary_master = txnPoolNodeSet[1].replicas[0]
    old_pp = non_primary_master.spylog.count(non_primary_master.processPrePrepare)

    master_primary._do_send_3pc_batch(DOMAIN_LEDGER_ID)
    looper.run(eventually(pp_processed, non_primary_master, old_pp))
    waitForViewChange(looper, txnPoolNodeSet, old_view + 1)

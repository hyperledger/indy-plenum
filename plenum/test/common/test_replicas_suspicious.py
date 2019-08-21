from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.server.view_change.view_changer import ViewChanger

from plenum.test.helper import waitForViewChange
from stp_core.loop.eventually import eventually


def test_nodes_make_view_change_only_on_master_suspicious(
        looper, txnPoolNodeSet):
    old_view = txnPoolNodeSet[0].viewNo

    master_primary = txnPoolNodeSet[0].replicas[0]
    backup_primary = txnPoolNodeSet[1].replicas[1]
    assert master_primary.isPrimary is True
    assert backup_primary.isPrimary is True

    master_primary._ordering_service.replica_batch_digest = lambda reqs: 'asd'
    backup_primary._ordering_service.replica_batch_digest = lambda reqs: 'asd'

    non_primary_backup = txnPoolNodeSet[0].replicas[1]
    old_pp = non_primary_backup.spylog.count(non_primary_backup._ordering_service.process_preprepare)

    def pp_processed(replica, old_pp):
        assert replica._ordering_service.spylog.count(replica._ordering_service.process_preprepare) == old_pp + 1

    backup_primary._ordering_service._do_send_3pc_batch(DOMAIN_LEDGER_ID)
    looper.run(eventually(pp_processed, non_primary_backup, old_pp))
    assert all(node.view_changer.spylog.count(ViewChanger.sendInstanceChange) == 0
               for node in txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, old_view)

    non_primary_master = txnPoolNodeSet[1].replicas[0]
    old_pp = non_primary_master.spylog.count(non_primary_master._ordering_service.process_preprepare)

    master_primary._ordering_service._do_send_3pc_batch(DOMAIN_LEDGER_ID)
    looper.run(eventually(pp_processed, non_primary_master, old_pp))
    waitForViewChange(looper, txnPoolNodeSet, old_view + 1)

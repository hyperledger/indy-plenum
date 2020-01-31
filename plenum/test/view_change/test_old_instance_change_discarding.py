import pytest

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.helper import send_test_instance_change
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    old_interval = tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL
    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = 10
    yield tconf

    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = old_interval


def test_old_instance_change_discarding(txnPoolNodeSet,
                                        looper,
                                        tconf):
    view_no = txnPoolNodeSet[0].viewNo
    first_nodes = txnPoolNodeSet[:2]
    second_nodes = txnPoolNodeSet[2:]

    for node in first_nodes:
        send_test_instance_change(node)

    def chk_ic_discard():
        for n in txnPoolNodeSet:
            instance_changes = n.master_replica._view_change_trigger_service._instance_changes
            assert not instance_changes.has_view(view_no + 1)
            for frm in first_nodes:
                assert not instance_changes.has_inst_chng_from(view_no + 1, frm.name)

    looper.run(eventually(chk_ic_discard,
                          timeout=tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 10))

    for node in second_nodes:
        send_test_instance_change(node)

    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    for node in txnPoolNodeSet:
        assert node.viewNo == view_no

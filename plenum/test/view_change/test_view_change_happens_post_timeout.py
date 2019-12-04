import pytest

from plenum.server.suspicion_codes import Suspicion, Suspicions
from plenum.test.test_node import getNonPrimaryReplicas


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldViewChangeWindowSize = tconf.ViewChangeWindowSize
    tconf.ViewChangeWindowSize = 5

    def reset():
        tconf.ViewChangeWindowSize = oldViewChangeWindowSize

    request.addfinalizer(reset)
    return tconf


def is_instance_change_sent_for_view_no(node, view_no):
    return node.master_replica._view_change_trigger_service._instance_changes.has_view(view_no)


def test_instance_change_happens_post_timeout(tconf, looper, txnPoolNodeSet):
    non_prim_node = getNonPrimaryReplicas(txnPoolNodeSet)[0].node
    old_view_no = non_prim_node.viewNo
    vct_service = non_prim_node.master_replica._view_change_trigger_service

    # first sending on InstanceChange: OK
    new_view_no = old_view_no + 1
    assert not is_instance_change_sent_for_view_no(non_prim_node, new_view_no)
    vct_service._send_instance_change(new_view_no, Suspicions.PRIMARY_DEGRADED)
    assert is_instance_change_sent_for_view_no(non_prim_node, new_view_no)

    # second immediate sending on InstanceChange: OK
    new_view_no = new_view_no + 1
    assert not is_instance_change_sent_for_view_no(non_prim_node, new_view_no)
    vct_service._send_instance_change(new_view_no, Suspicions.PRIMARY_DEGRADED)
    assert is_instance_change_sent_for_view_no(non_prim_node, new_view_no)

    # third sending on InstanceChange after ViewChangeWindowSize timeout: OK
    new_view_no = new_view_no + 1
    looper.runFor(tconf.ViewChangeWindowSize)
    assert not is_instance_change_sent_for_view_no(non_prim_node, new_view_no)
    vct_service._send_instance_change(new_view_no, Suspicions.PRIMARY_DEGRADED)
    assert is_instance_change_sent_for_view_no(non_prim_node, new_view_no)

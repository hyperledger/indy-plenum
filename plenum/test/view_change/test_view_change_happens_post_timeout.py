import pytest
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
    return node.view_changer.instanceChanges.hasView(view_no)


def test_instance_change_happens_post_timeout(tconf, looper, txnPoolNodeSet):
    non_prim_node = getNonPrimaryReplicas(txnPoolNodeSet)[0].node
    old_view_no = non_prim_node.viewNo

    # first sending on InstanceChange: OK
    new_view_no = old_view_no + 1
    assert not is_instance_change_sent_for_view_no(non_prim_node, new_view_no)
    non_prim_node.view_changer.sendInstanceChange(new_view_no)
    assert is_instance_change_sent_for_view_no(non_prim_node, new_view_no)

    # second immediate sending on InstanceChange: NOT OK
    new_view_no = new_view_no + 1
    assert not is_instance_change_sent_for_view_no(non_prim_node, new_view_no)
    non_prim_node.view_changer.sendInstanceChange(new_view_no)
    assert not is_instance_change_sent_for_view_no(non_prim_node, new_view_no)

    # third sending on InstanceChange after ViewChangeWindowSize timepout: OK
    new_view_no = new_view_no + 1
    looper.runFor(tconf.ViewChangeWindowSize)
    assert not is_instance_change_sent_for_view_no(non_prim_node, new_view_no)
    non_prim_node.view_changer.sendInstanceChange(new_view_no)
    assert is_instance_change_sent_for_view_no(non_prim_node, new_view_no)

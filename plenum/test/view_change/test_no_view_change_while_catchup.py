import pytest

from plenum.common.messages.node_messages import InstanceChange
from plenum.common.startable import Mode
from plenum.test.helper import checkViewNoForNodes, waitForViewChange
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import do_view_change, revert_do_view_change
from plenum.test.waits import expectedPoolViewChangeStartedTimeout
from stp_core.loop.eventually import eventually


@pytest.fixture(params=[Mode.starting, Mode.discovering, Mode.discovered, Mode.syncing])
def mode(request):
    return request.param


def check_instance_change_count(nodes, expected_count):
    for node in nodes:
        ic_count = sum(1 for msg in node.view_changer.inBox if isinstance(msg, InstanceChange))
        assert expected_count == ic_count


def try_view_change(looper, nodes):
    for node in nodes:
        looper.run(eventually(node.view_changer.serviceQueues))


def check_no_view_change(looper, nodes):
    looper.run(eventually(check_instance_change_count, nodes, 3,
                          timeout=expectedPoolViewChangeStartedTimeout(len(nodes))))
    try_view_change(looper, nodes)
    check_instance_change_count(nodes, 3)


def test_no_view_change_until_synced(txnPoolNodeSet, looper, mode):
    # emulate catchup by setting non-synced status
    for node in txnPoolNodeSet:
        node.mode = mode

    check_instance_change_count(txnPoolNodeSet, 0)

    # start View Change
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    old_meths = do_view_change(txnPoolNodeSet)
    for node in txnPoolNodeSet:
        node.view_changer.sendInstanceChange(old_view_no + 1)

    # make sure View Change is not started
    check_no_view_change(looper, txnPoolNodeSet)
    assert old_view_no == checkViewNoForNodes(txnPoolNodeSet)

    # emulate finishing of catchup by setting Participating status
    revert_do_view_change(txnPoolNodeSet, old_meths)
    for node in txnPoolNodeSet:
        node.mode = Mode.participating

    # make sure that View Change happened
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=old_view_no + 1)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

import pytest

from plenum.common.startable import Mode
from plenum.test.helper import checkViewNoForNodes, waitForViewChange
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import do_view_change, revert_do_view_change
from plenum.test.waits import expectedPoolViewChangeStartedTimeout


@pytest.fixture(params=['starting', 'discovering', 'discovered', 'syncing'])
def mode(request):
    if request.param == 'starting':
        return Mode.starting
    elif request.param == 'discovering':
        return Mode.discovering
    elif request.param == 'discovered':
        return Mode.discovered
    elif request.param == 'syncing':
        return Mode.syncing


def test_no_view_change_until_synced(txnPoolNodeSet, looper, mode):
    # emulate catchup by setting non-synced status
    for node in txnPoolNodeSet:
        node.mode = mode

    # start View Change
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    old_meths = do_view_change(txnPoolNodeSet)
    for node in txnPoolNodeSet:
        node.view_changer.sendInstanceChange(old_view_no + 1)
    looper.runFor(expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet)))

    # make sure View Change is not started
    assert old_view_no == checkViewNoForNodes(txnPoolNodeSet)

    # emulate finishing of catchup by setting Participating status
    revert_do_view_change(txnPoolNodeSet, old_meths)
    for node in txnPoolNodeSet:
        node.mode = Mode.participating

    # make sure that View Change happened
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=old_view_no + 1)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

import pytest

from plenum.common.messages.node_messages import InstanceChange
from plenum.common.startable import Mode
from plenum.common.stashing_router import UnsortedStash
from plenum.server.replica_validator_enums import STASH_CATCH_UP
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import checkViewNoForNodes, waitForViewChange
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import do_view_change, revert_do_view_change
from plenum.test.waits import expectedPoolViewChangeStartedTimeout
from stp_core.loop.eventually import eventually


@pytest.fixture(params=[Mode.starting, Mode.discovering, Mode.discovered, Mode.syncing])
def mode(request):
    return request.param


def check_stashed_instance_changes(nodes, expected_count):
    for node in nodes:
        queue = node.master_replica.stasher._queues.get(STASH_CATCH_UP)
        ic_count = sum(1 for msg in queue if isinstance(msg[0], InstanceChange)) \
            if queue is not None else 0
        assert expected_count == ic_count


def check_no_view_change(looper, nodes):
    looper.run(eventually(check_stashed_instance_changes, nodes, 3,
                          timeout=expectedPoolViewChangeStartedTimeout(len(nodes))))


def test_no_view_change_until_synced(txnPoolNodeSet, looper, mode):
    # emulate catchup by setting non-synced status
    for node in txnPoolNodeSet:
        node.mode = mode

    check_stashed_instance_changes(txnPoolNodeSet, 0)

    # start View Change
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    old_meths = do_view_change(txnPoolNodeSet)
    for node in txnPoolNodeSet:
        vct_service = node.master_replica._view_change_trigger_service
        vct_service._send_instance_change(old_view_no + 1, Suspicions.PRIMARY_DEGRADED)

    # make sure View Change is not started
    check_no_view_change(looper, txnPoolNodeSet)
    assert old_view_no == checkViewNoForNodes(txnPoolNodeSet)

    # emulate finishing of catchup by setting Participating status
    revert_do_view_change(txnPoolNodeSet, old_meths)
    for node in txnPoolNodeSet:
        node.mode = Mode.participating
        node.master_replica.stasher.process_all_stashed(STASH_CATCH_UP)

    # make sure that View Change happened
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=old_view_no + 1)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

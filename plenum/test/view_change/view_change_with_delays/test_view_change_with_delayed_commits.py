import pytest

from plenum.common.util import max_3PC_key
from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_random_and_check, sdk_send_random_request, sdk_get_reply
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually

# This is needed only with current view change implementation to give enough time
# to show what is exactly broken
TestRunningTimeLimitSec = 300


@pytest.fixture(scope="module")
def tconf(tconf):
    """
    Patch config so that monitor won't start view change unexpectedly
    """
    old_unsafe = tconf.unsafe
    tconf.unsafe.add("disable_view_change")
    yield tconf
    tconf.unsafe = old_unsafe


def last_prepared_certificate(nodes):
    """
    Find last prepared certificate in pool.
    When we don't have any request ordered in new view last_prepared_certificate_in_view()
    returns None, but in order to ease maths (like being able to use max_3PC_key, or calculating
    next expected 3PC key) this value is replaced with (view_no, 0).
    """

    def patched_last_prepared_certificate(n):
        result = n.master_replica.last_prepared_certificate_in_view()
        if result is None:
            result = (n.master_replica.viewNo, 0)
        return result

    return max_3PC_key(patched_last_prepared_certificate(n) for n in nodes)


def check_last_prepared_certificate(nodes, num):
    # Check that last_prepared_certificate reaches some 3PC key on N-f nodes
    assert sum(1 for n in nodes if n.master_replica.last_prepared_certificate_in_view() == num) >= 3


def check_view_change_done(nodes, view_no):
    # Check that view change is done and view_no is not less than target
    for n in nodes:
        assert n.master_replica.viewNo >= view_no
        assert n.master_replica.last_prepared_before_view_change is None


def do_view_change_with_pending_request_and_one_fast_node(fast_node,
                                                          nodes, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Perform view change while processing request, with one node receiving commits much sooner than others.
    With current implementation of view change this will result in corrupted state of fast node
    """

    fast_stasher = fast_node.nodeIbStasher

    slow_nodes = [n for n in nodes if n != fast_node]
    slow_stashers = [n.nodeIbStasher for n in slow_nodes]

    # Get last prepared certificate in pool
    lpc = last_prepared_certificate(nodes)
    # Get pool current view no
    view_no = lpc[0]

    # Delay all COMMITs
    with delay_rules(slow_stashers, cDelay()):
        with delay_rules(fast_stasher, cDelay()):
            # Send request
            request = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

            # Wait until this request is prepared on N-f nodes
            looper.run(eventually(check_last_prepared_certificate, nodes, (lpc[0], lpc[1] + 1)))

            # Trigger view change
            for n in nodes:
                n.view_changer.on_master_degradation()

        # Now commits are processed on fast node
        # Wait until view change is complete
        looper.run(eventually(check_view_change_done, nodes, view_no + 1, timeout=60))

    # Finish request gracefully
    sdk_get_reply(looper, request)


@pytest.mark.skip(reason="INDY-1303, case 2 (simplified)")
def test_view_change_with_delayed_commits(txnPoolNodeSet, looper,
                                          sdk_pool_handle,
                                          sdk_wallet_client,
                                          tconf):
    # Perform view change with Delta acting as fast node
    # With current view change implementation its state will become different from other nodes
    do_view_change_with_pending_request_and_one_fast_node(txnPoolNodeSet[3], txnPoolNodeSet,
                                                          looper, sdk_pool_handle, sdk_wallet_client)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


@pytest.mark.skip(reason="INDY-1303, case 2")
def test_two_view_changes_with_delayed_commits(txnPoolNodeSet, looper,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               tconf):
    # Perform view change with Delta acting as fast node
    # With current view change implementation its state will become different from other nodes
    do_view_change_with_pending_request_and_one_fast_node(txnPoolNodeSet[3], txnPoolNodeSet,
                                                          looper, sdk_pool_handle, sdk_wallet_client)

    # Perform view change with Alpha acting as fast node
    # With current view change implementation its state will become different from other nodes,
    # resulting in pool losing consensus and failing to finish view change at all
    do_view_change_with_pending_request_and_one_fast_node(txnPoolNodeSet[0], txnPoolNodeSet,
                                                          looper, sdk_pool_handle, sdk_wallet_client)

    # Check that pool can write transactions
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

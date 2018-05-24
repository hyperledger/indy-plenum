import pytest

from plenum.test.delayers import cDelay, pDelay
from plenum.test.helper import sdk_send_random_request, sdk_get_reply
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    """
    Patch config so that monitor won't start view change unexpectedly.
    Also increase minimum catchup timeout to some big value to fail tests
    that attempt to wait for view change.
    """
    old_unsafe = tconf.unsafe
    old_catchup_timeout = tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    old_viewchange_timeout = tconf.VIEW_CHANGE_TIMEOUT
    tconf.unsafe.add("disable_view_change")
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = 150
    tconf.VIEW_CHANGE_TIMEOUT = 300
    yield tconf
    tconf.unsafe = old_unsafe
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = old_catchup_timeout
    tconf.VIEW_CHANGE_TIMEOUT = old_viewchange_timeout


def check_last_prepared_certificate(nodes, num):
    for n in nodes:
        assert n.master_replica.last_prepared_certificate_in_view() == num


def check_view_change_done(nodes, view_no):
    # Check that view change is done and view_no is not less than target
    for n in nodes:
        assert n.master_replica.viewNo >= view_no
        assert n.master_replica.last_prepared_before_view_change is None


def do_view_change_with_unaligned_prepare_certificates(
        slow_nodes, nodes, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Perform view change with some nodes reaching lower last prepared certificate than others.
    With current implementation of view change this can result with view change taking a lot of time.
    """
    fast_nodes = [n for n in nodes if n not in slow_nodes]

    all_stashers = [n.nodeIbStasher for n in nodes]
    slow_stashers = [n.nodeIbStasher for n in slow_nodes]

    # Delay some PREPAREs and all COMMITs
    with delay_rules(slow_stashers, pDelay()):
        with delay_rules(all_stashers, cDelay()):
            # Send request
            request = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

            # Wait until this request is prepared on fast nodes
            looper.run(eventually(check_last_prepared_certificate, fast_nodes, (0, 1)))
            # Make sure its not prepared on slow nodes
            looper.run(eventually(check_last_prepared_certificate, slow_nodes, None))

            # Trigger view change
            for n in nodes:
                n.view_changer.on_master_degradation()

        # Now commits are processed
        # Wait until view change is complete
        looper.run(eventually(check_view_change_done, nodes, 1, timeout=60))

    # Finish request gracefully
    sdk_get_reply(looper, request)


def test_view_change_with_one_slow_node(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client, tconf):
    """
    Perform view change with only one node reaching lower last prepared certificate than others.
    This is to ensure that despite setting MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE high view change
    can finish early in optimistic scenarios.
    """
    do_view_change_with_unaligned_prepare_certificates(txnPoolNodeSet[3:],
                                                       txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


@pytest.mark.skip(reason="INDY-1303, case 1, also it can hurt jenkins!")
def test_view_change_with_unaligned_prepare_certificates_on_half_nodes(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client, tconf):
    """
    Perform view change with half nodes reaching lower last prepared certificate than others.
    With current implementation of view change this can result with view change taking a lot of time.
    """
    do_view_change_with_unaligned_prepare_certificates(txnPoolNodeSet[2:],
                                                       txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

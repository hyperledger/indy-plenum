import pytest

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change.view_change_with_delays.helper import  \
    do_view_change_with_unaligned_prepare_certificates

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

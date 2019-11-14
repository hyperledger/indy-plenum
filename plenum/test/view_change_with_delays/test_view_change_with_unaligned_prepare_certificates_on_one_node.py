import pytest

from plenum.test.helper import view_change_timeout, perf_monitor_disabled
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change_with_delays.helper import  \
    do_view_change_with_unaligned_prepare_certificates

@pytest.fixture(scope="module")
def tconf(tconf):
    """
    Patch config so that monitor won't start view change unexpectedly.
    Also increase minimum catchup timeout to some big value to fail tests
    that attempt to wait for view change.
    """
    with view_change_timeout(tconf, 300), perf_monitor_disabled(tconf):
        yield tconf


def test_view_change_with_unaligned_prepare_certificates_on_one_node(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client, tconf):
    """
    Perform view change with only one node reaching lower last prepared certificate than others.
    """
    do_view_change_with_unaligned_prepare_certificates(txnPoolNodeSet[3:],
                                                       txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

from plenum.test.view_change.conftest import perf_chk_patched
from plenum.test.helper import sdk_send_random_and_check

# Perf check is invoked manually in this test so prevent scheduled perf check
# as it affects the result of subsequent manual perf checks
PerfCheckFreq = 60
nodeCount = 4
whitelist = ["discarding message"]


def test_not_check_if_no_new_requests(perf_chk_patched, looper, txnPoolNodeSet,
                                      sdk_wallet_client, sdk_pool_handle):
    """
    Checks that node does not do performance check if there were no new
    requests since previous check
    """

    # Ensure that nodes participating, because otherwise they do not do check
    for node in txnPoolNodeSet:
        assert node.isParticipating

    # Check that first performance checks passes, but further do not
    for node in txnPoolNodeSet:
        assert node.checkPerformance() is not None
        assert node.checkPerformance() is None
        assert node.checkPerformance() is None
        assert node.checkPerformance() is None

    # Send new request and check that after it nodes can do
    # performance check again
    num_requests = 1
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              num_requests)
    for node in txnPoolNodeSet:
        assert node.checkPerformance() is not None

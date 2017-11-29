from plenum.test.sdk.conftest import sdk_send_signed_requests, sdk_get_replies
from plenum.test.batching_3pc.helper import checkNodesHaveSameRoots
from plenum.test import waits


def send_and_check(signed_reqs, looper, txnPoolNodeSet, pool_h, timeout = None):
    if not timeout:
        timeout_per_request = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
        # here we try to take into account what timeout for execution
        # N request - total_timeout should be in
        # timeout_per_request < total_timeout < timeout_per_request * N
        # we cannot just take (timeout_per_request * N) because it is so huge.
        # (for timeout_per_request=5 and N=10, total_timeout=50sec)
        # lets start with some simple formula:
        timeout = (1 + len(signed_reqs) / 10) * timeout_per_request

    results = sdk_send_signed_requests(pool_h, signed_reqs)
    sdk_get_replies(looper, results, timeout=timeout)
    checkNodesHaveSameRoots(txnPoolNodeSet)
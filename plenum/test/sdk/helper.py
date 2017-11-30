from plenum.test.sdk.conftest import sdk_send_signed_requests, sdk_get_replies, sdk_send_random_requests
from plenum.test.batching_3pc.helper import checkNodesHaveSameRoots
from plenum.test import waits


def send_and_check(signed_reqs, looper, txnPoolNodeSet, pool_h, timeout=None):
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


def send_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet, count,
                          customTimeoutPerReq: float = None, add_delay_to_timeout: float = 0,
                          override_timeout_limit=False, total_timeout=None):
    if not total_timeout:
        node_count = len(txnPoolNodeSet)
        timeout_per_request = customTimeoutPerReq or waits.expectedTransactionExecutionTime(node_count)
        timeout_per_request += add_delay_to_timeout
        # here we try to take into account what timeout for execution
        # N request - total_timeout should be in
        # timeout_per_request < total_timeout < timeout_per_request * N
        # we cannot just take (timeout_per_request * N) because it is so huge.
        # (for timeout_per_request=5 and N=10, total_timeout=50sec)
        # lets start with some simple formula:
        total_timeout = (1 + count / 10) * timeout_per_request

    sdk_reqs = sdk_send_random_requests(looper, sdk_pool, sdk_wallet, count)
    sdk_repl = sdk_get_replies(looper, sdk_reqs, timeout=total_timeout)
    return sdk_repl


def send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet, num_reqs, num_batches=1, **kwargs):
    # This method assumes that `num_reqs` <= num_batches*MaxbatchSize
    if num_batches == 1:
        return send_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet, num_reqs, **kwargs)

    sdk_resps = []
    for _ in range(num_batches - 1):
        sdk_resps.extend(send_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet,
                                                  num_reqs // num_batches, **kwargs))
    rem = num_reqs % num_batches
    if rem == 0:
        rem = num_reqs // num_batches
    sdk_resps.extend(send_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet, rem, **kwargs))
    return sdk_resps

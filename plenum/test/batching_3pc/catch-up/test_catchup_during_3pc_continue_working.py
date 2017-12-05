import pytest
from plenum.test.batching_3pc.helper import add_txns_to_ledger_before_order, checkNodesHaveSameRoots
from plenum.test.test_node import getNonPrimaryReplicas
import json
from plenum.test.helper import sdk_signed_random_requests, sdk_send_and_check


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldSize = tconf.Max3PCBatchSize
    oldTimeout = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = 10
    tconf.Max3PCBatchWait = 1

    def reset():
        tconf.Max3PCBatchSize = oldSize
        tconf.Max3PCBatchWait = oldTimeout

    request.addfinalizer(reset)
    return tconf


def test_catchup_during_3pc_continue_sending(tconf, looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, tconf.Max3PCBatchSize + 2)
    non_primary_replica = getNonPrimaryReplicas(txnPoolNodeSet, instId=0)[0]

    # Simulate catch-up (add txns to ledger):
    # add txns corresponding to the requests after we got enough COMMITs to
    # order, but before ordering.
    add_txns_to_ledger_before_order(
        non_primary_replica, [json.loads(req) for req in reqs[:tconf.Max3PCBatchSize]])

    sdk_send_and_check(reqs, looper, txnPoolNodeSet, sdk_pool_handle)
    checkNodesHaveSameRoots(txnPoolNodeSet)

    # send another requests and check that they are received
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, 2 * tconf.Max3PCBatchSize - 2)
    sdk_send_and_check(reqs, looper, txnPoolNodeSet, sdk_pool_handle)
    checkNodesHaveSameRoots(txnPoolNodeSet)

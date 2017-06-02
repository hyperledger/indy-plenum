from plenum.test.batching_3pc.helper import send_and_check, \
    add_txns_to_ledger_before_order
from plenum.test.helper import signed_random_requests
from plenum.test.test_node import getNonPrimaryReplicas


def test_catchup_during_3pc(tconf, looper, txnPoolNodeSet, client,
                            wallet1):
    reqs = signed_random_requests(wallet1, tconf.Max3PCBatchSize)
    non_primary_replica = getNonPrimaryReplicas(txnPoolNodeSet, instId=0)[0]

    # Simulate catch-up (add txns to ledger):
    # add txns corresponding to the requests after we got enough COMMITs to order, but before ordering.
    add_txns_to_ledger_before_order(non_primary_replica, reqs)

    send_and_check(reqs, looper, txnPoolNodeSet, client)

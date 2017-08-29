from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test import waits
from plenum.test.delayers import nom_delay, delay_3pc_messages
from plenum.test.helper import sendRandomRequests, \
    waitForSufficientRepliesForRequests
from plenum.test.batching_3pc.conftest import tconf
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually
from plenum.test.view_change.conftest import perf_chk_patched


def test_all_replicas_hold_request_keys(
        perf_chk_patched,
        looper,
        txnPoolNodeSet,
        client1,
        wallet1,
        client1Connected):
    """
    All replicas whether primary or non primary hold request keys of forwarded
    requests. Once requests are ordered, they request keys are removed from replica.
    """
    tconf = perf_chk_patched
    delay_3pc = 2
    delay_3pc_messages(txnPoolNodeSet, 0, delay_3pc)
    delay_3pc_messages(txnPoolNodeSet, 1, delay_3pc)

    def chk(count):
        # All replicas have same amount of forwarded request keys and all keys
        # are finalised.
        for node in txnPoolNodeSet:
            for r in node.replicas:
                if r.isPrimary is False:
                    assert len(r.requestQueues[DOMAIN_LEDGER_ID]) == count
                    for i in range(count):
                        k = r.requestQueues[DOMAIN_LEDGER_ID][i]
                        assert r.requests[k].finalised
                elif r.isPrimary is True:
                    assert len(r.requestQueues[DOMAIN_LEDGER_ID]) == 0

    reqs = sendRandomRequests(wallet1, client1, tconf.Max3PCBatchSize - 1)
    # Only non primary replicas should have all request keys with them
    looper.run(eventually(chk, tconf.Max3PCBatchSize - 1))
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs,
                                        add_delay_to_timeout=delay_3pc)
    # Replicas should have no request keys with them since they are ordered
    looper.run(eventually(chk, 0))  # Need to wait since one node might not
    # have processed it.

    delay = 1
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(nom_delay(delay))

    ensure_view_change(looper, txnPoolNodeSet)
    reqs = sendRandomRequests(wallet1, client1, 2 * tconf.Max3PCBatchSize)
    looper.run(eventually(chk, 2 * tconf.Max3PCBatchSize))

    # Since each nomination is delayed and there will be multiple nominations
    # so adding some extra time
    timeout = waits.expectedPoolElectionTimeout(len(txnPoolNodeSet)) + \
        len(txnPoolNodeSet) * delay
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=timeout)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs,
                                        add_delay_to_timeout=delay_3pc)
    looper.run(eventually(chk, 0))

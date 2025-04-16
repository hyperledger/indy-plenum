from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import delay_3pc_messages
from plenum.test.batching_3pc.conftest import tconf
from stp_core.loop.eventually import eventually
from plenum.test.view_change.conftest import perf_chk_patched
from plenum.test.helper import vdr_send_signed_requests, vdr_get_replies, \
    vdr_signed_random_requests, vdr_eval_timeout


def test_all_replicas_hold_request_keys(
        perf_chk_patched,
        looper,
        txnPoolNodeSet,
        vdr_wallet_client,
        vdr_pool_handle):
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
            for r in node.replicas.values():
                if r.isPrimary is False:
                    assert len(r._ordering_service.requestQueues[DOMAIN_LEDGER_ID]) == count
                    for i in range(count):
                        k = r._ordering_service.requestQueues[DOMAIN_LEDGER_ID][i]
                        assert r.requests[k].finalised
                elif r.isPrimary is True:
                    assert len(r._ordering_service.requestQueues[DOMAIN_LEDGER_ID]) == 0

    reqs = vdr_signed_random_requests(looper,
                                      vdr_wallet_client,
                                      tconf.Max3PCBatchSize - 1)
    req_resps = vdr_send_signed_requests(vdr_pool_handle, reqs, looper)
    # Only non primary replicas should have all request keys with them
    looper.run(eventually(chk, tconf.Max3PCBatchSize - 1))
    vdr_get_replies(looper, req_resps, timeout=vdr_eval_timeout(
        tconf.Max3PCBatchSize - 1, len(txnPoolNodeSet),
        add_delay_to_timeout=delay_3pc))
    # Replicas should have no request keys with them since they are ordered
    looper.run(eventually(chk, 0))  # Need to wait since one node might not
    # have processed it.

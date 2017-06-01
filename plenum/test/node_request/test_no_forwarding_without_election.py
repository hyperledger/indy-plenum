from plenum.test import waits
from plenum.test.delayers import nom_delay
from plenum.test.helper import sendRandomRequests, \
    waitForSufficientRepliesForRequests, sendReqsToNodesAndVerifySuffReplies
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually


def test_node_stashes_requests_if_no_primary(looper, txnPoolNodeSet, client1,
                                wallet1, client1Connected):
    """
    Node stashes requests while no primary is present, but once primary is
    determined, the stashed requests are processed
    """
    def chk_stashed(stashed):
        for node in txnPoolNodeSet:
            assert (len(node.reqs_stashed_for_primary) == 0) != stashed

    # No requests are stashed before and after sending any requests
    chk_stashed(False)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)
    chk_stashed(False)

    delay = 3
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(nom_delay(delay))

    # Ensure view change and soon as view starts, send requests
    ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)

    reqs = sendRandomRequests(wallet1, client1, 2)

    # The above requests must be stashed
    looper.run(eventually(chk_stashed, True, retryWait=.1,
                          timeout=3))

    # The elections must complete for the new view, though the election would
    # take longer since nominates are delayed. The calculation below is approx.
    timeout = waits.expectedPoolElectionTimeout(len(txnPoolNodeSet)) + \
              delay*(len(txnPoolNodeSet))
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=timeout)

    # The requests should be successful
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    # No requests should be stashed in propagator.
    chk_stashed(False)

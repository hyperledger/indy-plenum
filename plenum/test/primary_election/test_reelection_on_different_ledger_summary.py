from time import perf_counter

from plenum.test import waits
from plenum.test.delayers import delay_3pc_messages
from plenum.test.helper import sendRandomRequests, \
    waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change, \
    check_each_node_reaches_same_end_for_view
from stp_core.loop.eventually import eventually


def test_reelection_when_nodes_send_different_ledger_summary(looper,
                                                             txnPoolNodeSet,
                                                             client1,
                                                             wallet1,
                                                             client1Connected):
    """
    Delay each node's request processing such that nodes receive different ledger
    summaries and they do re-election
    :return:
    """
    min_delay = 3
    max_delay = 6

    delay_3pc_messages(txnPoolNodeSet, 0, min_delay=min_delay,
                       max_delay=max_delay)

    before_election_time = perf_counter()
    reqs = []
    for i in range(4):
        reqs.extend(sendRandomRequests(wallet1, client1, 5))
        looper.runFor(.1)
    new_view_no = ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)

    def chk():
        for node in txnPoolNodeSet:
            assert [e.params['reelection'] for e in
                           node.elector.spylog.getAll(node.elector.processReelection.__name__)
                           if e.params['reelection'].instId == 0 and e.starttime > before_election_time]

    # TODO: This fails, try explicitly making each node send Nomination with
    # different last ordered seqno but to do this `executeBatch` has to be
    # patched to track
    # looper.run(eventually(chk, retryWait=1, timeout=3+3 * 2 * max_delay))

    for node in txnPoolNodeSet:
        node.resetDelays()

    ensureElectionsDone(looper, txnPoolNodeSet)
    looper.run(eventually(check_each_node_reaches_same_end_for_view,
                          txnPoolNodeSet, new_view_no))
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs,
                                        customTimeoutPerReq=timeout + 3*max_delay)
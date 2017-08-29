from plenum.test import waits
from plenum.test.batching_3pc.helper import check_uncommitteds_equal
from stp_core.loop.eventually import eventually

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import delay_3pc_messages, \
    reset_delays_and_process_delayeds
from plenum.test.helper import sendRandomRequests, waitForViewChange, \
    send_reqs_to_nodes_and_verify_all_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import get_master_primary_node

Max3PCBatchSize = 3
from plenum.test.batching_3pc.conftest import tconf
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper


TestRunningTimeLimitSec = 200


def test_view_change_on_start(tconf, txnPoolNodeSet, looper, wallet1,
                              client1, client1Connected):
    """
    Do view change on a without any requests
    """
    old_view_no = txnPoolNodeSet[0].viewNo
    master_primary = get_master_primary_node(txnPoolNodeSet)
    other_nodes = [n for n in txnPoolNodeSet if n != master_primary]
    delay_3pc = 10
    delay_3pc_messages(txnPoolNodeSet, 0, delay_3pc)
    sent_batches = 2
    sendRandomRequests(wallet1, client1,
                       sent_batches * tconf.Max3PCBatchSize)

    def chk1():
        t_root, s_root = check_uncommitteds_equal(other_nodes)
        assert master_primary.domainLedger.uncommittedRootHash != t_root
        assert master_primary.states[DOMAIN_LEDGER_ID].headHash != s_root

    looper.run(eventually(chk1, retryWait=1))
    timeout = tconf.PerfCheckFreq + \
        waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))
    waitForViewChange(looper, txnPoolNodeSet, old_view_no + 1,
                      customTimeout=timeout)

    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    check_uncommitteds_equal(txnPoolNodeSet)

    reset_delays_and_process_delayeds(txnPoolNodeSet)
    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1,
                                              2 * Max3PCBatchSize,
                                              add_delay_to_timeout=delay_3pc)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

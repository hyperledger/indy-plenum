import sys
from plenum.test.delayers import cDelay
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually
from plenum.test.helper import sdk_send_random_and_check, sdk_send_batches_of_random_and_check, \
    waitForViewChange
from plenum.test.checkpoints.conftest import chkFreqPatched

CHK_FREQ = 2


def test_replica_clear_collections_after_view_change(looper,
                                                     txnPoolNodeSet,
                                                     sdk_pool_handle,
                                                     sdk_wallet_client,
                                                     tconf,
                                                     tdir,
                                                     allPluginsPath,
                                                     sdk_wallet_steward,
                                                     chkFreqPatched):
    """
    1. Delay commits on one instance.
    2. Order a transaction on the master.
    3. Do View Change.
    4. Send 2 batches for finalize checkpoint and cleaning requests queues.
    5. Check that requests from node contains all items from requestsQueue.
    """

    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]
    with delay_rules(stashers, cDelay(delay=sys.maxsize, instId=1)):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_steward, 1)

        for node in txnPoolNodeSet:
            node.view_changer.on_master_degradation()

        waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=1,
                          customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)

    sdk_send_batches_of_random_and_check(looper,
                                         txnPoolNodeSet,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         num_reqs=1 * CHK_FREQ,
                                         num_batches=2)

    def check_request_queues():
        assert len(txnPoolNodeSet[0].requests) == 0
        for reqs in txnPoolNodeSet[0].replicas[1].requestQueues.values():
            assert all(req in txnPoolNodeSet[0].requests for req in reqs)
    looper.run(eventually(check_request_queues))

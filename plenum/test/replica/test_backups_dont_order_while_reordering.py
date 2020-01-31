import pytest

from plenum.server.replica_helper import generateName
from plenum.server.replicas import MASTER_REPLICA_INDEX
from plenum.test.delayers import pDelay, cDelay, msg_req_delay, msg_rep_delay, old_view_pp_request_delay, ppDelay
from plenum.test.helper import sdk_send_random_and_check, sdk_send_random_requests
from plenum.test.stasher import delay_rules_without_processing, delay_rules
from plenum.test.test_node import check_not_in_view_change
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

REQS_FOR_REORDERING = 10
BACKUP_INST_ID = 1


@pytest.fixture(scope="module")
def tconf(tconf):
    old_b_size = tconf.Max3PCBatchSize
    old_b_in_flight = tconf.Max3PCBatchesInFlight

    tconf.Max3PCBatchSize = 1
    tconf.Max3PCBatchesInFlight = REQS_FOR_REORDERING
    yield tconf

    tconf.Max3PCBatchSize = old_b_size
    tconf.Max3PCBatchesInFlight = old_b_in_flight


def check_req_queue(node, expected_req_count):
    assert len(node.requests) == expected_req_count


def test_backups_dont_order_while_reordering(txnPoolNodeSet,
                                             sdk_pool_handle,
                                             sdk_wallet_client,
                                             looper):
    """
    This test needs to show that for now we stop ordering on backups
    until master in reordering state after view_change
    Steps:
    1. Delay ordering on master replica for collecting requests to reorder after VC
    2. Make sure that master didn't order
    3. Delay old_view_pp_request and force VC
    4. Ensure that all backup replica on all nodes cannot order
       because primary waiting for reordering on master
    """

    def check_pp_count(node, expected_count, inst_id=0):
        assert node.replicas._replicas[inst_id].last_ordered_3pc[1] == expected_count, \
            "master last ordered: {}, backup_last_ordered: {}".format(node.master_replica._ordering_service.batches,
                                                                      node.replicas._replicas[
                                                                          1]._ordering_service.batches)

    # We expect that after VC Gamma will be primary on backup
    delayed_node = txnPoolNodeSet[-2]
    fast_nodes = [n for n in txnPoolNodeSet if n != delayed_node]
    master_pp_seq_no_before = delayed_node.master_replica.last_ordered_3pc[1]
    with delay_rules_without_processing(delayed_node.nodeIbStasher,
                                        pDelay(instId=MASTER_REPLICA_INDEX),
                                        cDelay(instId=MASTER_REPLICA_INDEX),
                                        msg_req_delay(),
                                        msg_rep_delay(),
                                        ppDelay(instId=MASTER_REPLICA_INDEX)):
        sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, REQS_FOR_REORDERING)
        looper.run(eventually(check_pp_count, delayed_node, REQS_FOR_REORDERING, BACKUP_INST_ID))
        assert delayed_node.master_replica.last_ordered_3pc[1] == master_pp_seq_no_before
        with delay_rules([n.nodeIbStasher for n in txnPoolNodeSet], old_view_pp_request_delay()):
            ensure_view_change(looper, txnPoolNodeSet)

            # check that view change is finished on all nodes
            looper.run(eventually(check_not_in_view_change, txnPoolNodeSet))

            # check that delayed node is selected on all fast nodes but not on delayed node
            def check_backup_primaries():
                assert delayed_node.replicas[BACKUP_INST_ID]._consensus_data.primary_name is None
                assert delayed_node.master_replica.last_ordered_3pc[1] == master_pp_seq_no_before
                assert all(
                    n.replicas[BACKUP_INST_ID]._consensus_data.primary_name == generateName(delayed_node.name,
                                                                                            instId=BACKUP_INST_ID)
                    for n in fast_nodes
                )

            looper.run(eventually(check_backup_primaries))

            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, REQS_FOR_REORDERING)
            for node in txnPoolNodeSet:
                assert node.replicas._replicas[BACKUP_INST_ID].last_ordered_3pc[1] == 0

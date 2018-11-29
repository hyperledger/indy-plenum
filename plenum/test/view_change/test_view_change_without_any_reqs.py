import pytest

from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.test import waits
from plenum.test.batching_3pc.helper import check_uncommitteds_equal
from stp_core.loop.eventually import eventually

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import delay_3pc_messages, \
    reset_delays_and_process_delayeds
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange, sdk_send_random_requests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import get_master_primary_node

Max3PCBatchSize = 3
from plenum.test.batching_3pc.conftest import tconf

TestRunningTimeLimitSec = 200


@pytest.fixture(scope="module")
def tconf(tconf):
    old_throughput_measurement_class = tconf.throughput_measurement_class
    old_throughput_measurement_params = tconf.throughput_measurement_params
    old_max_3pc_batch_size = tconf.Max3PCBatchSize
    old_timeout = tconf.ACC_MONITOR_TIMEOUT

    tconf.throughput_measurement_class = RevivalSpikeResistantEMAThroughputMeasurement
    tconf.throughput_measurement_params = {
        'window_size': 2,
        'min_cnt': 3
    }
    tconf.Max3PCBatchSize = Max3PCBatchSize
    tconf.ACC_MONITOR_TIMEOUT = 5

    yield tconf

    tconf.throughput_measurement_class = old_throughput_measurement_class
    tconf.throughput_measurement_params = old_throughput_measurement_params
    tconf.Max3PCBatchSize = old_max_3pc_batch_size
    tconf.ACC_MONITOR_TIMEOUT = old_timeout


def test_view_change_on_start(tconf, txnPoolNodeSet, looper,
                              sdk_pool_handle, sdk_wallet_client):
    """
    Do view change on a without any requests
    """
    old_view_no = txnPoolNodeSet[0].viewNo
    master_primary = get_master_primary_node(txnPoolNodeSet)
    other_nodes = [n for n in txnPoolNodeSet if n != master_primary]
    delay_3pc = 10
    delay_3pc_messages(txnPoolNodeSet, 0, delay_3pc)
    sent_batches = 2
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client,
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
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                              2 * Max3PCBatchSize, add_delay_to_timeout=delay_3pc)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

import sys

import pytest

from plenum.test.batching_3pc.helper import node_caughtup
from plenum.test.stasher import delay_rules

from stp_core.loop.eventually import eventually

from plenum.test.delayers import cDelay, pDelay, ppDelay, chk_delay
from plenum.test.helper import sdk_send_batches_of_random_and_check, \
    sdk_send_batches_of_random, max_3pc_batch_limits, assertExp

from plenum.test.checkpoints.conftest import chkFreqPatched, reqs_for_checkpoint

CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ

req_num = CHK_FREQ * 2
howlong = 100
ledger_id = 1
another_key = 'request_2'


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        old_type = tconf.METRICS_COLLECTOR_TYPE
        tconf.METRICS_COLLECTOR_TYPE = 'kv'
        yield tconf
        tconf.METRICS_COLLECTOR_TYPE = old_type


def test_freeing_forwarded_not_preprepared_request(
        looper, chkFreqPatched, reqs_for_checkpoint, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_steward, tconf, tdir, allPluginsPath):
    behind_node = txnPoolNodeSet[-1]
    behind_node.requests.clear()

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_steward, CHK_FREQ, CHK_FREQ)
    count = behind_node.spylog.count(behind_node.allLedgersCaughtUp)
    with delay_rules(behind_node.nodeIbStasher,
                     ppDelay(delay=sys.maxsize),
                     pDelay(delay=sys.maxsize),
                     cDelay(delay=sys.maxsize)):
        with delay_rules(behind_node.nodeIbStasher, chk_delay(delay=sys.maxsize)):
            sdk_send_batches_of_random(looper, txnPoolNodeSet, sdk_pool_handle,
                                       sdk_wallet_steward, req_num, req_num)
            looper.run(eventually(lambda: assertExp(len(behind_node.requests) == req_num)))
        # Start catchup with the quorum of Checkpoints
        looper.run(eventually(node_caughtup, behind_node, count, retryWait=1))
        assert all(r.executed for r in behind_node.requests.values() if behind_node.seqNoDB.
                   get_by_full_digest(r.request.key)[1])

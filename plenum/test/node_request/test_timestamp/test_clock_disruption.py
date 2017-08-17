import types
from random import randint

import pytest

from plenum.common.util import get_utc_epoch
from stp_core.loop.eventually import eventually

from plenum.test.helper import send_reqs_to_nodes_and_verify_all_replies, \
    sendRandomRequests
from plenum.test.node_request.test_timestamp.helper import make_clock_faulty, \
    get_timestamp_suspicion_count

Max3PCBatchSize = 4

from plenum.test.batching_3pc.conftest import tconf

# lot of requests will be sent and multiple view changes are done
TestRunningTimeLimitSec = 200


@pytest.mark.skip(reason='Pending implementation')
def test_nodes_with_bad_clock(tconf, looper, txnPoolNodeSet, client1,
                              wallet1, client1Connected):
    """
    All nodes have bad clocks but they eventaully get repaired, an example of
    nodes being cut off from NTP server for some time or NTP sync disabled
    then without node restart NTP sync enabled
    """
    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1,
                                              Max3PCBatchSize * 3)

    ledger_sizes = {node.name: node.domainLedger.size for node in
                    txnPoolNodeSet}
    susp_counts = {node.name: get_timestamp_suspicion_count(node) for node in
                   txnPoolNodeSet}
    for node in txnPoolNodeSet:
        make_clock_faulty(
            node,
            clock_slow_by_sec=node.config.ACCEPTABLE_DEVIATION_PREPREPARE_SECS +
            randint(
                5,
                15),
            ppr_always_wrong=False)

    for _ in range(5):
        sendRandomRequests(wallet1, client1, 2)
        looper.runFor(.2)

    # Let some time pass
    looper.runFor(3)

    def chk():
        for node in txnPoolNodeSet:
            # Each node raises suspicion
            assert get_timestamp_suspicion_count(node) > susp_counts[node.name]
            # Ledger does not change
            assert node.domainLedger.size == ledger_sizes[node.name]

    looper.run(eventually(chk, retryWait=1))

    # Fix clocks
    for node in txnPoolNodeSet:
        def utc_epoch(self) -> int:
            return get_utc_epoch()

        node.utc_epoch = types.MethodType(utc_epoch, node)

    # Let some more time pass
    looper.runFor(3)

    # All nodes reply
    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1,
                                              Max3PCBatchSize * 2)

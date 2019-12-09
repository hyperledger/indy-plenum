import pytest

from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.test.helper import sdk_send_random_requests
from stp_core.loop.eventually import eventually


howlong = 20
nodeCount = 8


@pytest.fixture(scope="module")
def tconf(tconf):
    OUTDATED_REQS_CHECK_ENABLED_OLD = tconf.OUTDATED_REQS_CHECK_ENABLED
    OUTDATED_REQS_CHECK_INTERVAL_OLD = tconf.OUTDATED_REQS_CHECK_INTERVAL
    PROPAGATES_PHASE_REQ_TIMEOUT_OLD = tconf.PROPAGATES_PHASE_REQ_TIMEOUT
    ORDERING_PHASE_REQ_TIMEOUT_OLD = tconf.ORDERING_PHASE_REQ_TIMEOUT
    REPLICAS_REMOVING_WITH_DEGRADATION_OLD = tconf.REPLICAS_REMOVING_WITH_DEGRADATION
    REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED_OLD = tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED

    tconf.OUTDATED_REQS_CHECK_ENABLED = True
    tconf.OUTDATED_REQS_CHECK_INTERVAL = 1
    tconf.PROPAGATES_PHASE_REQ_TIMEOUT = 3600
    tconf.ORDERING_PHASE_REQ_TIMEOUT = 20
    tconf.REPLICAS_REMOVING_WITH_DEGRADATION = None
    tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = None

    yield tconf

    tconf.OUTDATED_REQS_CHECK_ENABLED = OUTDATED_REQS_CHECK_ENABLED_OLD
    tconf.OUTDATED_REQS_CHECK_INTERVAL = OUTDATED_REQS_CHECK_INTERVAL_OLD
    tconf.PROPAGATES_PHASE_REQ_TIMEOUT = PROPAGATES_PHASE_REQ_TIMEOUT_OLD
    tconf.ORDERING_PHASE_REQ_TIMEOUT = ORDERING_PHASE_REQ_TIMEOUT_OLD
    tconf.REPLICAS_REMOVING_WITH_DEGRADATION = REPLICAS_REMOVING_WITH_DEGRADATION_OLD
    tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED_OLD


def test_req_drop_when_backup_replica_does_not_order(
        tconf, looper, txnPoolNodeSet,
        sdk_wallet_client, sdk_pool_handle):
    assert len(txnPoolNodeSet[0].replicas) == 3

    # Stop the primary of backup replica
    backup_primary_name_off = replica_name_to_node_name(txnPoolNodeSet[0].replicas[2].primaryName)
    for n in txnPoolNodeSet:
        if n.name == backup_primary_name_off:
            n.stop()

    initial_ledger_size = txnPoolNodeSet[0].domainLedger.size

    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)

    def check_request_queue():
        for n in txnPoolNodeSet:
            if n.name != backup_primary_name_off:
                assert len(n.requests) == 1
                assert n.propagates_phase_req_timeouts == 0
                assert n.ordering_phase_req_timeouts == 0

    timeout = howlong
    looper.run(eventually(check_request_queue, retryWait=.5, timeout=timeout))

    def check_drop():
        for n in txnPoolNodeSet:
            if n.name != backup_primary_name_off:
                assert len(n.requests) == 0
                assert n.propagates_phase_req_timeouts == 0
                assert n.ordering_phase_req_timeouts == 1

    timeout = tconf.ORDERING_PHASE_REQ_TIMEOUT + tconf.OUTDATED_REQS_CHECK_INTERVAL + 10
    looper.run(eventually(check_drop, retryWait=.5, timeout=timeout))

    def check_ledger_size():
        # The request should be eventually ordered
        for n in txnPoolNodeSet:
            if n.name != backup_primary_name_off:
                assert n.domainLedger.size - initial_ledger_size == 1

    looper.run(eventually(check_ledger_size, retryWait=.5, timeout=timeout))

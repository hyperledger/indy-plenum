import pytest

from plenum.common.constants import AUDIT_LEDGER_ID
from plenum.test.delayers import delay_3pc, cqDelay
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits, assert_eq
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import start_delaying, stop_delaying_and_process
from stp_core.loop.eventually import eventually

nodeCount = 7


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        old_catchup_txn_timeout = tconf.CatchupTransactionsTimeout
        old_catchup_batch_size = tconf.CATCHUP_BATCH_SIZE

        # Effectively disable resending CATCHUP_REQs, so if node sends some requests that
        # cannot be processed catchup won't finish
        tconf.CatchupTransactionsTimeout = 1000

        # Make catchup batch size small to increase probability of hitting all nodes
        # with catchup requests
        tconf.CATCHUP_BATCH_SIZE = 1

        yield tconf
        tconf.CatchupTransactionsTimeout = old_catchup_txn_timeout
        tconf.CATCHUP_BATCH_SIZE = old_catchup_batch_size


def test_catchup_uses_only_nodes_with_cons_proofs(looper,
                                                  txnPoolNodeSet,
                                                  sdk_pool_handle,
                                                  sdk_wallet_client):
    lagging_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]

    start_delaying(lagging_node.nodeIbStasher, delay_3pc())
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 10)

    catchup_reqs = {node.name: start_delaying(node.nodeIbStasher, cqDelay())
                    for node in other_nodes}
    audit_catchup_service = lagging_node.ledgerManager._node_leecher._leechers[AUDIT_LEDGER_ID]._catchup_rep_service
    lagging_node.start_catchup()
    looper.run(eventually(lambda: assert_eq(audit_catchup_service._is_working, True)))

    # Make sure number of cons proofs gathered when all nodes are
    assert len(audit_catchup_service._nodes_ledger_sizes) == 3

    # Allow catchup requests only for interesting nodes
    for node_id in audit_catchup_service._nodes_ledger_sizes.keys():
        stop_delaying_and_process(catchup_reqs[node_id])

    # Check catchup finishes successfully
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=30)

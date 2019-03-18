import types

import plenum.server.catchup.cons_proof_service as catchup_utils
from plenum.common.constants import CONSISTENCY_PROOF, CURRENT_PROTOCOL_VERSION, AUDIT_LEDGER_ID
from plenum.common.ledger import Ledger
from plenum.server.catchup.utils import CatchupDataProvider
from plenum.test.delayers import ppDelay, pDelay, cDelay
from plenum.test.node_request.message_request.helper import \
    count_msg_reqs_of_type
from plenum.test.stasher import delay_rules
from stp_core.common.log import getlogger
from plenum.common.messages.node_messages import LedgerStatus
from plenum.test.helper import sdk_send_random_requests, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, ensure_all_nodes_have_same_data

# Do not remove the next imports
from plenum.test.node_catchup.conftest import whitelist
from plenum.test.batching_3pc.conftest import tconf

logger = getlogger()
# So that `three_phase_key_for_txn_seq_no` always works, it makes the test
# easy as the requesting node selects a random size for the ledger
Max3PCBatchSize = 1
TestRunningTimeLimitSec = 150


def test_node_request_consistency_proof(tdir, tconf,
                                        looper,
                                        txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_client,
                                        monkeypatch):
    lagging_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]

    # Preseed pool with some transactions
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 4)

    # Make some node send different ledger statuses so it doesn't get enough similar
    # consisistency proofs
    next_size = 0
    orig_method = catchup_utils.build_ledger_status

    def build_broken_ledger_status(ledger_id: int, provider: CatchupDataProvider):
        nonlocal next_size
        if provider.node_name() != lagging_node.name:
            return orig_method(ledger_id, provider)
        if ledger_id != AUDIT_LEDGER_ID:
            return orig_method(ledger_id, provider)

        audit_ledger = provider.ledger(AUDIT_LEDGER_ID)

        size = audit_ledger.size
        next_size = next_size + 1 if next_size < size else 1
        print("new size {}".format(next_size))

        newRootHash = Ledger.hashToStr(audit_ledger.tree.merkle_tree_hash(0, next_size))
        three_pc_key = provider.three_phase_key_for_txn_seq_no(ledger_id, next_size)
        v, p = three_pc_key if three_pc_key else (None, None)
        ledgerStatus = LedgerStatus(AUDIT_LEDGER_ID, next_size, v, p, newRootHash,
                                    CURRENT_PROTOCOL_VERSION)
        logger.info("audit status {}".format(ledgerStatus))
        return ledgerStatus

    monkeypatch.setattr(catchup_utils, 'build_ledger_status', build_broken_ledger_status)
    logger.info('Audit Ledger status sender of {} patched'.format(lagging_node))

    # Block lagging node from ordering transactions
    with delay_rules(lagging_node.nodeIbStasher, ppDelay(), pDelay(), cDelay()):
        # Order some transactions on pool
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 4)

        # Start catchup on lagging node
        lagging_node.ledgerManager.start_catchup()

        # Wait until catchup is succesfully finished
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=75)

        # Make sure that there were requests for consistency proofs on other pool
        for node in other_nodes:
             assert count_msg_reqs_of_type(node, CONSISTENCY_PROOF) > 0, node

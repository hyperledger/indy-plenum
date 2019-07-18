import pytest

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.constants import AUDIT_LEDGER_ID, LEDGER_STATUS, COMMIT, CONSISTENCY_PROOF
from plenum.common.messages.node_messages import LedgerStatus, MessageRep, ConsistencyProof
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import delay_3pc, lsDelay, msg_rep_delay, msg_req_delay, cpDelay
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits, assertExp, MockTimer
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules_without_processing, delay_rules
from plenum.test.test_node import checkNodesConnected, TestNode
from stp_core.loop.eventually import eventually

#
#
# @pytest.fixture(scope="module")
# def txnPoolNodeSet(txnPoolNodeSet):
#     for node in txnPoolNodeSet:
#         node.timer = MockTimer(int(node.timer.get_current_time()))
#     return txnPoolNodeSet
from stp_core.types import HA


@pytest.fixture(scope="module")
def tconf(tconf):
    tmp = tconf.LedgerStatusTimeout

    tconf.LedgerStatusTimeout = 0.5

    yield tconf
    tconf.LedgerStatusTimeout = tmp


def test_catchup_with_reask_ls(txnPoolNodeSet,
                               looper,
                               sdk_pool_handle,
                               sdk_wallet_steward,
                               tconf,
                               tdir,
                               allPluginsPath,
                               monkeypatch):
    '''
    Start a catchup
    Delay MessageqResp with LedgerStatuses twice
    Check that the catchup finished
    '''
    node_to_disconnect = txnPoolNodeSet[-1]
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)
    node_to_disconnect.nodeIbStasher.delay(msg_rep_delay(types_to_delay=[COMMIT]))

    with delay_rules_without_processing(node_to_disconnect.nodeIbStasher, delay_3pc(),
                                        msg_rep_delay(types_to_delay=[COMMIT])):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward,
                                  2)
        node_to_disconnect.nodeIbStasher.drop_delayeds()
    with delay_rules_without_processing(node_to_disconnect.nodeIbStasher,
                                        lsDelay(),
                                        msg_rep_delay(types_to_delay=[LEDGER_STATUS])):
        node_to_disconnect.start_catchup()

        def chk():
            resp_ls_count = 0
            for msg in node_to_disconnect.nodeIbStasher.delayeds:
                if isinstance(msg.item[0], MessageRep) and msg.item[0].msg_type == LEDGER_STATUS:
                    resp_ls_count += 1
            assert resp_ls_count >= (len(txnPoolNodeSet) - 1) * 2
            node_to_disconnect.nodeIbStasher.drop_delayeds()

        looper.run(eventually(chk))
    waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet,
                         exclude_from_check=['check_last_ordered_3pc_backup'])

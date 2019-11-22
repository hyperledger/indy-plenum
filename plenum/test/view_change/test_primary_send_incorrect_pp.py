import pytest

from plenum.common.messages.node_messages import OldViewPrePrepareReply, PrePrepare
from plenum.test.delayers import lsDelay, msg_rep_delay, ppDelay
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules, delay_rules_without_processing
from plenum.test.view_change.helper import ensure_all_nodes_have_same_data, \
    ensure_view_change, add_new_node
from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState, POOL_LEDGER_ID, LEDGER_STATUS, PREPREPARE
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange, assertExp, sdk_send_random_request, \
    sdk_get_and_check_replies

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.node_catchup.helper import check_ledger_state, \
    waitNodeDataEquality
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node, sdk_pool_refresh
from plenum.test import waits
from plenum.common.startable import Mode

logger = getlogger()


def test_primary_send_incorrect_pp(looper, txnPoolNodeSet, tconf,
                                   allPluginsPath, sdk_pool_handle,
                                   sdk_wallet_steward,
                                   monkeypatch):
    """
    Test steps:
    Delay PrePrepares on `slow_node` (without processing)
    Delay receiving of OldViewPrePrepareRequest on all nodes but `malicious_node`
    Patch sending for OldViewPrePrepareReply on the `malicious_node` to send an invalid PrePrepare
    Start a view change
    Make sure it's finished on all nodes excluding `slow_node`
    Make sure that the lagging node received OldViewPrePrepareReply from the malicious node
    Reset delay for OldViewPrePrepareRequest  on other nodes
    Make sure the pool is functional and all nodes have same data
    """
    start_view_no = txnPoolNodeSet[0].viewNo
    slow_node = txnPoolNodeSet[-1]
    malicious_node = txnPoolNodeSet[0]
    other_nodes = [n for n in txnPoolNodeSet if n not in [slow_node, malicious_node]]
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(txnPoolNodeSet))
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=timeout)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)
    old_sender = malicious_node.master_replica._ordering_service._send

    def patched_sender(msg, dst=None, stat=None):
        if isinstance(msg, PrePrepare) and msg:
            old_sender(msg, [n.name for n in other_nodes], stat)
            msg.ppTime += 1
            old_sender(msg, [slow_node.name], stat)

    monkeypatch.setattr(malicious_node.master_replica._ordering_service,
                        '_send',
                        patched_sender)
    monkeypatch.setattr(slow_node.master_replica._ordering_service,
                            '_validate_applied_pre_prepare',
                            lambda a, b, c: None)
    next_seq_no = slow_node.master_last_ordered_3PC[1] + 1
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_steward)

    def chk():
        assert (start_view_no, next_seq_no) in slow_node.master_replica._ordering_service.prePrepares

    looper.run(eventually(chk))
    monkeypatch.undo()


    _, j_resp = sdk_get_and_check_replies(looper, [resp_task])[0]
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)
    # for n in txnPoolNodeSet:
    #     for ledger in n.db_manager._ledgers.values():
    #         print("{} {}".format(n.name, ledger.root_hash))

    for n in txnPoolNodeSet:
        n.view_changer.on_master_degradation()
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=start_view_no + 1)

    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet,
                        instances_list=[0, 1])
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)

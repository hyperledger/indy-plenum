import pytest

from plenum.common.messages.node_messages import PrePrepare
from plenum.server.consensus.ordering_service import OrderingService
from plenum.test.delayers import msg_rep_delay
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from plenum.test.view_change.helper import ensure_all_nodes_have_same_data
from plenum.common.constants import PREPREPARE
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange, sdk_send_random_request, \
    sdk_get_and_check_replies
from plenum.test.view_change_service.helper import trigger_view_change

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.test_node import ensureElectionsDone
from plenum.test import waits

logger = getlogger()


def test_primary_send_incorrect_pp(looper, txnPoolNodeSet, tconf,
                                   allPluginsPath, sdk_pool_handle,
                                   sdk_wallet_steward,
                                   monkeypatch):
    """
    Test steps:
    Delay message requests with PrePrepares on `slow_node`
    Patch sending for PrePrepare on the `malicious_primary` to send an invalid PrePrepare to slow_node
    Order a new request
    Start a view change
    Make sure it's finished on all nodes
    Make sure that the lagging node has same data with other nodes
    """
    start_view_no = txnPoolNodeSet[0].viewNo
    slow_node = txnPoolNodeSet[-1]
    malicious_primary = txnPoolNodeSet[0]
    other_nodes = [n for n in txnPoolNodeSet if n not in [slow_node, malicious_primary]]
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(txnPoolNodeSet))
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=timeout)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)
    old_sender = malicious_primary.master_replica._ordering_service._send

    def patched_sender(msg, dst=None, stat=None):
        if isinstance(msg, PrePrepare) and msg:
            old_sender(msg, [n.name for n in other_nodes], stat)
            pp_dict = msg._asdict()
            pp_dict["ppTime"] += 1
            pp = PrePrepare(**pp_dict)
            old_sender(pp, [slow_node.name], stat)
            monkeypatch.undo()

    monkeypatch.setattr(malicious_primary.master_replica._ordering_service,
                        '_send',
                        patched_sender)
    monkeypatch.setattr(slow_node.master_replica._ordering_service,
                        '_validate_applied_pre_prepare',
                        lambda a, b, c: None)
    with delay_rules(slow_node.nodeIbStasher, msg_rep_delay(types_to_delay=[PREPREPARE])):
        preprepare_process_num = slow_node.master_replica._ordering_service.spylog.count(
            OrderingService.process_preprepare)
        resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_steward)

        def chk():
            assert preprepare_process_num + 1 == slow_node.master_replica._ordering_service.spylog.count(
                OrderingService.process_preprepare)

        looper.run(eventually(chk))

        _, j_resp = sdk_get_and_check_replies(looper, [resp_task])[0]
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 1)

        trigger_view_change(txnPoolNodeSet)
        ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=start_view_no + 1)

        ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet,
                            instances_list=[0, 1])
        ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
        sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)

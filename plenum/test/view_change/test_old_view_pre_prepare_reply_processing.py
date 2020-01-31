from plenum.common.messages.node_messages import OldViewPrePrepareReply, PrePrepare
from plenum.server.consensus.ordering_service import OrderingService
from plenum.test.delayers import msg_rep_delay, ppDelay, old_view_pp_request_delay
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules, delay_rules_without_processing
from plenum.test.view_change.helper import ensure_all_nodes_have_same_data
from plenum.common.constants import PREPREPARE
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange
from plenum.test.view_change_service.helper import trigger_view_change

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.test_node import ensureElectionsDone
from plenum.test import waits

logger = getlogger()
nodeCount = 7


def test_old_view_pre_prepare_reply_processing(looper, txnPoolNodeSet, tconf,
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
    slow_node = txnPoolNodeSet[-2]
    malicious_node = txnPoolNodeSet[-1]
    other_nodes = [n for n in txnPoolNodeSet if n not in [slow_node, malicious_node]]
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=tconf.NEW_VIEW_TIMEOUT)
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(txnPoolNodeSet))
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=timeout)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)

    with delay_rules_without_processing(slow_node.nodeIbStasher, ppDelay(),
                                        msg_rep_delay(types_to_delay=[PREPREPARE])):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 1)
    with delay_rules([n.nodeIbStasher for n in other_nodes], old_view_pp_request_delay()):
        old_sender = malicious_node.master_replica._ordering_service._send

        def patched_sender(msg, dst=None, stat=None):
            if isinstance(msg, OldViewPrePrepareReply) and msg.preprepares:
                pp_dict = msg.preprepares[0]._asdict()
                pp_dict["digest"] = "incorrect_digest"
                pp = PrePrepare(**pp_dict)
                msg.preprepares[0] = pp
                monkeypatch.undo()
            old_sender(msg, dst, stat)

        monkeypatch.setattr(malicious_node.master_replica._ordering_service,
                            '_send',
                            patched_sender)
        monkeypatch.setattr(slow_node.master_replica._ordering_service,
                            '_validate_applied_pre_prepare',
                            lambda a, b, c: None)
        process_old_pp_num = slow_node.master_replica._ordering_service.spylog.count(
            OrderingService.process_old_view_preprepare_reply)

        trigger_view_change(txnPoolNodeSet)

        waitForViewChange(looper, other_nodes + [malicious_node], expectedViewNo=start_view_no + 1)

        ensureElectionsDone(looper=looper, nodes=other_nodes + [malicious_node],
                            instances_list=[0, 1, 2])
        ensure_all_nodes_have_same_data(looper, nodes=other_nodes + [malicious_node])

        def chk():
            assert process_old_pp_num + 1 == slow_node.master_replica._ordering_service.spylog.count(
                OrderingService.process_old_view_preprepare_reply)
        looper.run(eventually(chk))

    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
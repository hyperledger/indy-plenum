import pytest

from plenum.common.exceptions import RequestNackedException
from plenum.common.messages.node_messages import InstanceChange
from plenum.common.startable import Mode
from plenum.test.delayers import lsDelay, vcd_delay
from plenum.test.helper import checkViewNoForNodes, waitForViewChange, \
    sdk_send_random_requests, sdk_get_replies, sdk_get_and_check_replies, \
    sdk_send_random_and_check, sdk_signed_random_requests, sdk_gen_request, \
    checkDiscardMsg
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone, TestNode
from plenum.test.view_change.helper import do_view_change, revert_do_view_change
from plenum.test.waits import expectedPoolViewChangeStartedTimeout
from stp_core.loop.eventually import eventually


def test_client_msg_discard_in_view_change_integration(txnPoolNodeSet,
                                                       looper,
                                                       sdk_pool_handle,
                                                       sdk_wallet_client):
    '''
    Check that client requests sent in view change will discard.
    '''
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 4)
    for node in txnPoolNodeSet:
        for node_for_send in txnPoolNodeSet:
            node.view_changer.instanceChanges.addVote(InstanceChange(1, 25),
                                                      node_for_send.name)
    # Delay ViewChangeDone to send client request in view change.
    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]
    with delay_rules(stashers, vcd_delay(10)):
        for node in txnPoolNodeSet:
            node.view_changer.on_master_degradation()
        discard_reqs = sdk_send_random_requests(looper, sdk_pool_handle,
                                                sdk_wallet_client, 4)
    ensureElectionsDone(looper, txnPoolNodeSet)

    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, discard_reqs)
        assert "Client request is discarded since view " \
               "change is in progress" in e.args[0]


def test_client_msg_discard_in_view_change_unit(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    node.view_changer.view_change_in_progress = True
    msg = sdk_gen_request("op").as_dict
    node.unpackClientMsg(msg, "frm")
    checkDiscardMsg([node, ], msg, "view change in progress")

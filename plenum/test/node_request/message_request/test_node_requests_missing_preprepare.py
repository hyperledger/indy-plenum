import types

import pytest

from plenum.common.constants import PREPREPARE
from plenum.common.types import MessageReq, MessageRep, f
from plenum.common.util import check_if_all_equal_in_list, updateNamedTuple
from plenum.test.delayers import ppDelay
from plenum.test.helper import send_reqs_batches_and_get_suff_replies, \
    send_reqs_to_nodes_and_verify_all_replies, countDiscarded
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import getNonPrimaryReplicas, get_master_primary_node


whitelist = ['does not have expected state']


@pytest.fixture(scope="module")
def teardown(request, looper, txnPoolNodeSet, client1, wallet1):

    def tear():
        # Repair any broken network
        for node in txnPoolNodeSet:
            node.reset_delays_and_process_delayeds()
        # Give a little time to process any delayed messages
        looper.runFor(3)

        # Check each node has same data
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

        # Check each node has ordered all requests (no catchup)
        assert check_if_all_equal_in_list([n.master_replica.ordered
                                           for n in txnPoolNodeSet])

        # Check the network is functional since all nodes reply
        send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 5)

    request.addfinalizer(tear)


def test_node_requests_missing_preprepare(looper, txnPoolNodeSet, client1,
                                          wallet1, client1Connected, teardown):
    """
    A node has bad network with primary and thus loses PRE-PREPARE,
    it requests PRE-PREPARE from non-primaries once it has sufficient PREPAREs
    """
    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]

    # Delay PRE-PREPAREs by large amount simulating loss
    slow_node.nodeIbStasher.delay(ppDelay(300, 0))
    old_count_pp = get_count(slow_node.master_replica,
                          slow_node.master_replica.processPrePrepare)
    old_count_mrq = {n.name: get_count(n, n.process_message_req)
                     for n in other_nodes}
    old_count_mrp = get_count(slow_node, slow_node.process_message_rep)

    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1, 15, 5)

    waitNodeDataEquality(looper, slow_node, *other_nodes)

    assert not slow_node.master_replica.requested_pre_prepares

    # `slow_node` processed PRE-PREPARE
    assert get_count(slow_node.master_replica,
                     slow_node.master_replica.processPrePrepare) > old_count_pp

    # `slow_node` did receive `MessageRep`
    assert get_count(slow_node, slow_node.process_message_rep) > old_count_mrp

    # More than `f` nodes received `MessageReq`
    recv_reqs = set()
    for n in other_nodes:
        if get_count(n, n.process_message_req) > old_count_mrq[n.name]:
            recv_reqs.add(n.name)

    assert len(recv_reqs) > slow_node.f

    # All nodes including the `slow_node` ordered the same requests
    assert check_if_all_equal_in_list([n.master_replica.ordered
                                       for n in txnPoolNodeSet])


@pytest.fixture(scope='module', params=['do_not_send', 'send_bad'])
def malicious_setup(request, txnPoolNodeSet):
    primary_node = get_master_primary_node(txnPoolNodeSet)
    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    bad_node = [n for n in other_nodes if n != primary_node][0]
    good_non_primary_node = [n for n in other_nodes if n != slow_node
                             and n != bad_node and n != primary_node][0]

    if request.param == 'do_not_send':
        orig_method = bad_node.nodeMsgRouter.routes[MessageReq]

        def do_not_send(self, msg, frm):
            if msg.msg_type == PREPREPARE:
                return
            else:
                return orig_method(msg, frm)

        bad_node.nodeMsgRouter.routes[MessageReq] = types.MethodType(do_not_send,
                                                                     bad_node)
        return primary_node, bad_node, good_non_primary_node, slow_node, \
               other_nodes, do_not_send, orig_method

    if request.param == 'send_bad':
        orig_method = bad_node.nodeMsgRouter.routes[MessageReq]

        def send_bad(self, msg, frm):
            if msg.msg_type == PREPREPARE:
                resp = self.replicas[msg.params['instId']].getPrePrepare(
                    msg.params['viewNo'], msg.params['ppSeqNo'])
                resp = updateNamedTuple(resp, digest='11908ffq')
                self.sendToNodes(MessageRep(**{
                    f.MSG_TYPE.nm: msg.msg_type,
                    f.PARAMS.nm: msg.params,
                    f.MSG.nm: resp
                }), names=[frm, ])
            else:
                return orig_method(msg, frm)

        bad_node.nodeMsgRouter.routes[MessageReq] = types.MethodType(send_bad,
                                                                     bad_node)
        return primary_node, bad_node, good_non_primary_node, slow_node, \
               other_nodes, send_bad, orig_method


def test_node_requests_missing_preprepare_malicious(looper, txnPoolNodeSet,
                                                    client1, wallet1,
                                                    client1Connected,
                                                    malicious_setup, teardown):
    """
    A node has bad network with primary and thus loses PRE-PREPARE,
    it requests PRE-PREPARE from non-primaries once it has sufficient PREPAREs
    but one of the non-primary does not send the PRE-PREPARE
    """
    # primary_node = get_master_primary_node(txnPoolNodeSet)
    # slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    # other_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    # bad_node = [n for n in other_nodes if n != primary_node][0]
    # good_non_primary_node = [n for n in other_nodes if n != slow_node
    #                          and n != bad_node and n != primary_node][0]
    primary_node, bad_node, good_non_primary_node, slow_node, other_nodes, \
    bad_method, orig_method = malicious_setup

    slow_node.nodeIbStasher.delay(ppDelay(300, 0))

    def get_reply_count_frm(node):
        return sum([1 for entry in slow_node.spylog.getAll(
            slow_node.process_message_rep)
                    if entry.params['msg'].msg_type == PREPREPARE and
                    entry.params['frm'] == node.name])

    old_reply_count_from_bad_node = get_reply_count_frm(bad_node)
    old_reply_count_from_good_node = get_reply_count_frm(good_non_primary_node)
    old_discarded = countDiscarded(slow_node.master_replica, 'does not have '
                                                        'expected state')

    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1, 10, 2)

    waitNodeDataEquality(looper, slow_node, *other_nodes)

    assert check_if_all_equal_in_list([n.master_replica.ordered
                                       for n in txnPoolNodeSet])

    assert not slow_node.master_replica.requested_pre_prepares

    if bad_method.__name__ == 'do_not_send':
        assert get_reply_count_frm(bad_node) == old_reply_count_from_bad_node
    else:
        assert countDiscarded(slow_node.master_replica,
                              'does not have expected state') > old_discarded

    assert get_reply_count_frm(good_non_primary_node) > \
           old_reply_count_from_good_node

    slow_node.reset_delays_and_process_delayeds()
    bad_node.nodeMsgRouter.routes[MessageReq] = orig_method

import types
from collections import defaultdict

from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import send_reqs_to_nodes_and_verify_all_replies, \
    getNodeSuspicions
from plenum.test.instances.helper import recvd_prepares
from plenum.test.node_request.test_timestamp.helper import \
    get_timestamp_suspicion_count
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas


whitelist = ['to have incorrect time']


def test_replicas_prepare_time(looper, txnPoolNodeSet, client1,
                                wallet1, client1Connected):
    # Check that each replica's PREPARE time is same as the PRE-PREPARE time
    sent_batches = 5
    for i in range(sent_batches):
        send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 2)
        looper.runFor(1)

    for node in txnPoolNodeSet:
        for r in node.replicas:
            rec_prps = defaultdict(list)
            for p in recvd_prepares(r):
                rec_prps[(p.viewNo, p.ppSeqNo)].append(p)
            pp_coll = r.sentPrePrepares if r.isPrimary else r.prePrepares
            for key, pp in pp_coll.items():
                for p in rec_prps[key]:
                    assert pp.ppTime == p.ppTime

            # `last_accepted_pre_prepare_time` is the time of the last PRE-PREPARE
            assert r.last_accepted_pre_prepare_time == pp_coll.peekitem(-1)[1].ppTime


def test_non_primary_accepts_pre_prepare_time(looper, txnPoolNodeSet, client1,
                                              wallet1, client1Connected):
    """
    One of the non-primary has an in-correct clock so it thinks PRE-PREPARE
    has incorrect time
    """
    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 2)
    # The replica having the bad clock
    confused_npr = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1]

    def ppr_time_is_wrong(self, pp):
        return False

    confused_npr.is_pre_prepare_time_correct = types.MethodType(
        ppr_time_is_wrong, confused_npr)

    old_acceptable_rvs = getAllReturnVals(confused_npr,
                                      confused_npr.is_pre_prepare_time_acceptable)
    old_susp_count = get_timestamp_suspicion_count(confused_npr.node)
    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 2)

    assert get_timestamp_suspicion_count(confused_npr.node) > old_susp_count

    new_acceptable_rvs = getAllReturnVals(confused_npr,
                                          confused_npr.is_pre_prepare_time_acceptable)

    # `is_pre_prepare_time_acceptable` first returned False then returned True
    assert [True, False, *old_acceptable_rvs] == new_acceptable_rvs


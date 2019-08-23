import base58

from plenum.test.helper import assertEquality


def checkRequestCounts(nodes, req_count, batches_count):
    for node in nodes:
        assertEquality(len(node.requests), req_count)
        for r in node.replicas.values():
            assertEquality(len(r._ordering_service.commits), batches_count)
            assertEquality(len(r._ordering_service.prepares), batches_count)
            assertEquality(len(r._ordering_service.batches), batches_count)


def check_stashed_chekpoints(node, count):
    c = sum(len(ckps)
            for ckps_for_view in node.master_replica._checkpointer._stashed_recvd_checkpoints.values()
            for ckps in ckps_for_view.values())
    assert count == c, "{} != {}".format(count, c)


def cp_digest(min: int, max: int, key: str = '0') -> str:
    assert len(key) == 1
    digest = "digest-{}-{}-".format(min, max)
    digest = digest + key * (32 - len(digest))
    return base58.b58encode(digest).decode()


def check_stable_checkpoint(replica, pp_seq_no):
    assert replica._consensus_data.stable_checkpoint == pp_seq_no, \
        "expected stable checkpoint {}, got {}".format(pp_seq_no, replica._consensus_data.stable_checkpoint)


def check_num_unstable_checkpoints(replica, num):
    # TODO: This should be actually == num + 1
    assert len(replica._consensus_data.checkpoints) == num


def check_last_checkpoint(replica, pp_seq_no, view_no=0):
    # TODO: View_no is going to be removed
    cp = replica._consensus_data.checkpoints[-1]
    assert cp.seqNoEnd == pp_seq_no
    assert cp.viewNo == view_no


def check_num_received_checkpoints(replica, num):
    assert sum(1 for votes in replica._checkpointer._received_checkpoints.values()
               if len(votes) > 0) == num


def check_num_quorumed_received_checkpoints(replica, num):
    assert sum(1 for cp in replica._checkpointer._received_checkpoints
               if replica._checkpointer._have_quorum_on_received_checkpoint(cp)) == num


def check_last_received_checkpoint(replica, pp_seq_no, view_no = 0):
    # TODO: View_no is going to be removed
    received_checkpoints = [cp for cp, votes in replica._checkpointer._received_checkpoints.items()
                            if len(votes) > 0]
    max_cp = max(received_checkpoints, key=lambda cp: (cp.view_no, cp.pp_seq_no))
    assert max_cp.pp_seq_no == pp_seq_no
    assert max_cp.view_no == view_no


def check_received_checkpoint_votes(replica, pp_seq_no, num_votes, view_no = 0):
    # TODO: View_no is going to be removed
    received_checkpoints = [votes for cp, votes in replica._checkpointer._received_checkpoints.items()
                            if len(votes) > 0 and cp.pp_seq_no == pp_seq_no and cp.view_no == view_no]
    assert len(received_checkpoints) == 1
    assert len(received_checkpoints[0].votes) == num_votes


def check_for_instance(nodes, inst_id, checker, *args, **kwargs):
    for node in nodes:
        r = node.replicas.values()[inst_id]
        checker(r, *args, **kwargs)


def check_for_nodes(nodes, checker, *args, **kwargs):
    for i in nodes[0].replicas.keys():
        check_for_instance(nodes, i, checker, *args, **kwargs)


# def chkChkpoints(nodes, total: int, stableIndex: int = None):
#     for i in nodes[0].replicas.keys():
#         chk_chkpoints_for_instance(nodes, i, total, stableIndex)
#
#
# def chk_chkpoints_for_instance(nodes, inst_id, total: int, stableIndex: int = None):
#     for node in nodes:
#         r = node.replicas.values()[inst_id]
#         assert len(r._consensus_data.checkpoints) == total, '{} checkpoints {}, whereas total {}'. \
#             format(r, len(r._consensus_data.checkpoints), total)
#         if stableIndex is not None:
#             assert r._checkpointer._checkpoint_state.values()[stableIndex].isStable, r.name
#         else:
#             for state in r._checkpointer._checkpoint_state.values():
#                 assert not state.isStable

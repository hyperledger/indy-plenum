import base58

from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.test.helper import assertEquality


def checkRequestCounts(nodes, req_count, batches_count):
    for node in nodes:
        assertEquality(len(node.requests), req_count)
        for r in node.replicas.values():
            assertEquality(len(r._ordering_service.commits), batches_count)
            assertEquality(len(r._ordering_service.prepares), batches_count)
            assertEquality(len(r._ordering_service.batches), batches_count)


def cp_digest(max: int, key: str = '0') -> str:
    assert len(key) == 1
    digest = "digest-{}-".format(max)
    digest = digest + key * (32 - len(digest))
    return base58.b58encode(digest).decode()


def check_stable_checkpoint(replica, pp_seq_no):
    assert replica._consensus_data.stable_checkpoint == pp_seq_no, \
        "{} expected stable checkpoint {}, got {}".format(replica.name, pp_seq_no, replica._consensus_data.stable_checkpoint)
    stable_checkpoints = list(replica._consensus_data.checkpoints.irange_key(min_key=pp_seq_no, max_key=pp_seq_no))
    assert len(stable_checkpoints) == 1, \
        "{} expected one stable checkpoint, got {}".format(replica.name, stable_checkpoints)


def check_num_unstable_checkpoints(replica, num):
    assert len(replica._consensus_data.checkpoints) == num + 1, \
        "expected {} unstable checkpoints, got {} (stable is {})".format(num,
                                                                         replica._consensus_data.checkpoints,
                                                                         replica._consensus_data.stable_checkpoint)


def check_last_checkpoint(replica, pp_seq_no, view_no=0):
    # TODO: View_no is going to be removed
    cp = replica._consensus_data.checkpoints[-1]
    assert cp.seqNoEnd == pp_seq_no
    assert cp.viewNo == view_no


def check_num_received_checkpoints(replica, num):
    num_received = sum(1
                       for votes in replica._checkpointer._received_checkpoints.values()
                       if len(votes) > 0)
    assert num_received == num, \
        "expected {} received checkpoints, got {}".format(num, num_received)


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
    assert len(received_checkpoints[0]) == num_votes


def check_for_instance(nodes, inst_id, checker, *args, **kwargs):
    for node in nodes:
        r = node.replicas.values()[inst_id]
        checker(r, *args, **kwargs)


def check_for_nodes(nodes, checker, *args, **kwargs):
    for i in nodes[0].replicas.keys():
        check_for_instance(nodes, i, checker, *args, **kwargs)


def cp_key(view_no: int, pp_seq_no: int) -> CheckpointService.CheckpointKey:
    return CheckpointService.CheckpointKey(view_no=view_no, pp_seq_no=pp_seq_no, digest=cp_digest(pp_seq_no))

import pytest

from plenum.common.messages.node_messages import Checkpoint, ViewChange
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.view_change_service import NewViewBuilder, BatchID
from plenum.test.greek import genNodeNames

N = 4
F = 1

TestRunningTimeLimitSec = 600


@pytest.fixture
def consensus_data_provider():
    validators = genNodeNames(N)
    return ConsensusSharedData("nodeA", validators, 0)


@pytest.fixture
def builder(consensus_data_provider):
    return NewViewBuilder(consensus_data_provider)


@pytest.fixture
def strong_quorum(consensus_data_provider):
    return consensus_data_provider.quorums.strong.value


def test_calc_batches_empty(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vcs = [
        ViewChange(viewNo=0, stableCheckpoint=0, prepared=[], preprepared=[], checkpoints=[cp]),
        ViewChange(viewNo=0, stableCheckpoint=0, prepared=[], preprepared=[], checkpoints=[cp]),
        ViewChange(viewNo=0, stableCheckpoint=0, prepared=[], preprepared=[], checkpoints=[cp]),
        ViewChange(viewNo=0, stableCheckpoint=0, prepared=[], preprepared=[], checkpoints=[cp]),
    ]
    assert [] == builder.calc_batches(cp, vcs)


def test_calc_batches_quorum(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                    preprepared=[(0, 1, "digest1"), (0, 2, "digest2"), (0, 3, "digest3")],
                    checkpoints=[cp])

    vcs = [vc]
    assert builder.calc_batches(cp, vcs) is None

    vcs.append(vc)
    assert builder.calc_batches(cp, vcs) is None

    vcs.append(vc)
    assert builder.calc_batches(cp, vcs)


def test_calc_batches_same_data(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                    preprepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                    checkpoints=[cp])

    vcs = [vc, vc, vc, vc]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1"), BatchID(0, 2, "digest2")]


def test_calc_batches_must_be_in_pre_prepare(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                    preprepared=[(0, 1, "digest1")],
                    checkpoints=[cp])

    vcs = [vc, vc, vc, vc]
    # all nodes are malicious here since all added (0, 2) into prepared without adding to pre-prepared
    # so, None here means we can not calculate NewView reliably
    assert builder.calc_batches(cp, vcs) is None

    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])
    vc2 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])

    vcs = [vc1, vc2, vc2, vc2]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1")]


def test_calc_batches_takes_prepared_only(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[],
                    preprepared=[(0, 1, "digest1"), (0, 2, "digest2"), (0, 3, "digest3"), (0, 4, "digest4")],
                    checkpoints=[cp])

    vcs = [vc, vc, vc, vc]
    assert builder.calc_batches(cp, vcs) == []

    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                    preprepared=[(0, 1, "digest1"), (0, 2, "digest2"), (0, 3, "digest3"), (0, 4, "digest4")],
                    checkpoints=[cp])

    vcs = [vc, vc, vc, vc]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1"), BatchID(0, 2, "digest2")]


def test_calc_batches_takes_max_view(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(0, 1, "digest1"), (0, 2, "digest2"), (1, 1, "digest1"), (1, 2, "digest2")],
                    preprepared=[(0, 1, "digest1"), (0, 2, "digest2"), (1, 1, "digest1"), (1, 2, "digest2")],
                    checkpoints=[cp])

    vcs = [vc, vc, vc, vc]
    assert builder.calc_batches(cp, vcs) == [BatchID(1, 1, "digest1"), BatchID(1, 2, "digest2")]


def test_calc_batches_respects_checkpoint(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                    preprepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                    checkpoints=[cp])

    vcs = [vc, vc, vc, vc]
    assert builder.calc_batches(cp, vcs) == []

    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(0, 10, "digest10"), (0, 11, "digest11"), (1, 12, "digest12")],
                    preprepared=[(0, 10, "digest10"), (0, 11, "digest11"), (1, 12, "digest12")],
                    checkpoints=[cp])

    vcs = [vc, vc, vc, vc]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 11, "digest11"), BatchID(1, 12, "digest12")]


def test_calc_batches_takes_quorum_of_prepared(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest2")],
                     preprepared=[(0, 1, "digest2")],
                     checkpoints=[cp])
    vc2 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])
    vc3 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])

    vcs = [vc1, vc2, vc2, vc2]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1")]

    vcs = [vc3, vc2, vc2, vc2]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1")]

    vcs = [vc3, vc3, vc3, vc3]
    assert builder.calc_batches(cp, vcs) == []

    vcs = [vc1, vc1, vc2, vc2]
    assert builder.calc_batches(cp, vcs) is None

    # since we have enough pre-prepares
    vcs = [vc2, vc3, vc3, vc3]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1")]
    vcs = [vc2, vc2, vc3, vc3]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1")]


def test_calc_batches_takes_one_prepared_if_weak_quorum_of_preprepared(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                     preprepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                     checkpoints=[cp])
    vc2 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1"), (0, 2, "digest2")],
                     checkpoints=[cp])
    vc3 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])
    vc4 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])

    vcs = [vc1, vc2, vc3, vc4]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1"), (0, 2, "digest2")]


def test_calc_batches_takes_next_view_one_prepared_if_weak_quorum_of_preprepared(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1"), (1, 2, "digest2")],
                     preprepared=[(0, 1, "digest1"), (1, 2, "digest2")],
                     checkpoints=[cp])
    vc2 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1"), (1, 2, "digest2")],
                     checkpoints=[cp])
    vc3 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])
    vc4 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])

    vcs = [vc1, vc2, vc3, vc4]
    assert builder.calc_batches(cp, vcs) == [BatchID(0, 1, "digest1"), (1, 2, "digest2")]


def test_calc_batches_takes_next_view_prepared_if_old_view_prepared(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest2")],
                     preprepared=[(0, 1, "digest1"), (1, 1, "digest2")],
                     checkpoints=[cp])
    vc2 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1"), (1, 1, "digest2")],
                     checkpoints=[cp])
    vc3 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])
    vc4 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])

    vcs = [vc1, vc2, vc3, vc4]
    assert builder.calc_batches(cp, vcs) == [BatchID(1, 1, "digest2")]


def test_calc_batches_takes_prepared_if_preprepared_in_next_view(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest2")],
                     preprepared=[(0, 1, "digest1"), (2, 1, "digest2")],
                     checkpoints=[cp])
    vc2 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1"), (2, 1, "digest2")],
                     checkpoints=[cp])
    vc3 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])
    vc4 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(0, 1, "digest1")],
                     preprepared=[(0, 1, "digest1")],
                     checkpoints=[cp])

    vcs = [vc1, vc2, vc3, vc4]
    assert builder.calc_batches(cp, vcs) == [BatchID(1, 1, "digest2")]


def test_calc_batches_takes_prepared_with_same_batchid_only(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest1")],
                     preprepared=[(1, 1, "digest1")],
                     checkpoints=[cp])
    vc2 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest1")],
                     preprepared=[(1, 1, "digest1")],
                     checkpoints=[cp])
    vc3 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest2")],
                     preprepared=[(1, 1, "digest2")],
                     checkpoints=[cp])
    vc4 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[],
                     preprepared=[(1, 1, "digest1")],
                     checkpoints=[cp])

    vcs = [vc1, vc2, vc3, vc4]
    assert builder.calc_batches(cp, vcs) == [BatchID(1, 1, "digest1")]

def test_calc_checkpints_empty(builder):
    vc =  ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest1")],
                     preprepared=[(1, 1, "digest1")],
                     checkpoints=[])
    vcs = [vc, vc, vc, vc]
    assert builder.calc_checkpoint(vcs) == 0

def test_calc_checkpints_equal_no_stable(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc =  ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest1")],
                     preprepared=[(1, 1, "digest1")],
                     checkpoints=[cp])
    vcs = [vc, vc, vc, vc]
    assert builder.calc_checkpoint(vcs) == 0

#
# def test_combinations(builder, random):
#     MAX_PP_SEQ_NO = 20
#     MAX_VIEW_NO = 3
#     MAX_DIGEST_ID = 20
#
#     cp = Checkpoint(instId=0, viewNo=1, seqNoStart=0, seqNoEnd=0, digest='empty')
#
#     batch_ids = [(viewno, pp_seq_no, "digest{}".format(digest_id))
#                  for viewno in range(1, MAX_VIEW_NO)
#                  for pp_seq_no in range(1, MAX_PP_SEQ_NO)
#                  for digest_id in range(1, MAX_DIGEST_ID)]
#
#     batch_combinations = [batches for r in range(MAX_PP_SEQ_NO - 1) for batches in combinations(batch_ids, r)]
#
#     view_change_msgs = [
#         ViewChange(
#             viewNo=2, stableCheckpoint=0, prepared=prep_comb, preprepared=pre_prep_comb, checkpoints=[cp]
#         )
#         for pre_prep_comb in batch_combinations
#         for prep_comb in batch_combinations
#     ]
#
#     # for vc_count in range(N - F):
#     #     for vcs in combinations(view_change_msgs, vc_count):
#     #         assert builder.calc_batches(cp, list(vcs)) is None
#
#     for i in range(100):
#         for vc_count in range(N - F, N + 1):
#             view_changes = random.sample(view_change_msgs, vc_count)
#
#             batches = builder.calc_batches(cp, list(view_changes))
#             committed = calc_committed(view_changes, MAX_PP_SEQ_NO, N, F)
#             print(committed)
#
#             if committed:
#                 if batches is None:
#                     print([(vc.preprepared, vc.prepared) for vc in view_changes])
#                     assert batches is not None
#                 assert committed == batches[:len(committed)]

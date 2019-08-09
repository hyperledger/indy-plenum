import pytest

from plenum.common.messages.node_messages import Checkpoint, ViewChange
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.view_change_service import NewViewBuilder, BatchID
from plenum.test.consensus.view_change.helper import calc_committed
from plenum.test.greek import genNodeNames
from plenum.test.simulation.sim_random import DefaultSimRandom

N = 4
F = 1

# TestRunningTimeLimitSec = 600


@pytest.fixture
def consensus_data_provider():
    validators = genNodeNames(N)
    return ConsensusSharedData("nodeA", validators, 0)


@pytest.fixture
def builder(consensus_data_provider):
    return NewViewBuilder(consensus_data_provider)


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


def test_calc_checkpoints_empty(builder):
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(1, 1, "digest1")],
                    preprepared=[(1, 1, "digest1")],
                    checkpoints=[])
    vcs = [vc, vc, vc, vc]
    assert builder.calc_checkpoint(vcs) is None


def test_calc_checkpoints_equal_initial(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(1, 1, "digest1")],
                    preprepared=[(1, 1, "digest1")],
                    checkpoints=[cp])
    vcs = [vc, vc, vc, vc]
    assert builder.calc_checkpoint(vcs) == cp


def test_calc_checkpoints_equal_no_stable(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=0,
                    prepared=[(1, 1, "digest1")],
                    preprepared=[(1, 1, "digest1")],
                    checkpoints=[cp])
    vcs = [vc, vc, vc, vc]
    assert builder.calc_checkpoint(vcs) == cp


def test_calc_checkpoints_equal_stable(builder):
    cp = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='empty')
    vc = ViewChange(viewNo=0, stableCheckpoint=10,
                    prepared=[(1, 1, "digest1")],
                    preprepared=[(1, 1, "digest1")],
                    checkpoints=[cp])
    vcs = [vc, vc, vc, vc]
    assert builder.calc_checkpoint(vcs) == cp


def test_calc_checkpoints_quorum(builder):
    cp1 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='d1')
    cp2 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='d2')

    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest1")],
                     preprepared=[(1, 1, "digest1")],
                     checkpoints=[cp1])
    vc2 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest1")],
                     preprepared=[(1, 1, "digest1")],
                     checkpoints=[cp2])
    vc2_stable = ViewChange(viewNo=0, stableCheckpoint=10,
                            prepared=[(1, 1, "digest1")],
                            preprepared=[(1, 1, "digest1")],
                            checkpoints=[cp2])

    vcs = [vc1, vc1, vc1, vc1]
    assert builder.calc_checkpoint(vcs) == cp1

    vcs = [vc2, vc2, vc2, vc2]
    assert builder.calc_checkpoint(vcs) == cp2

    vcs = [vc2_stable, vc2_stable, vc2_stable, vc2_stable]
    assert builder.calc_checkpoint(vcs) == cp2

    vcs = [vc2, vc1, vc1, vc1]
    assert builder.calc_checkpoint(vcs) == cp1

    vcs = [vc2, vc2, vc1, vc1]
    assert builder.calc_checkpoint(vcs) == cp2

    vcs = [vc2, vc2, vc2, vc1]
    assert builder.calc_checkpoint(vcs) == cp2

    vcs = [vc2_stable, vc1, vc1, vc1]
    assert builder.calc_checkpoint(vcs) == cp1

    vcs = [vc2_stable, vc2_stable, vc1, vc1]
    assert builder.calc_checkpoint(vcs) == cp2

    vcs = [vc2_stable, vc2_stable, vc2_stable, vc1]
    assert builder.calc_checkpoint(vcs) == cp2

    vcs = [vc2_stable, vc2, vc2, vc2]
    assert builder.calc_checkpoint(vcs) == cp2

    vcs = [vc2_stable, vc2_stable, vc2, vc2]
    assert builder.calc_checkpoint(vcs) == cp2

    vcs = [vc2_stable, vc2_stable, vc2_stable, vc2]
    assert builder.calc_checkpoint(vcs) == cp2


def test_calc_checkpoints_selects_max(builder):
    cp1 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='d1')
    cp2 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='d2')
    cp3 = Checkpoint(instId=0, viewNo=0, seqNoStart=10, seqNoEnd=20, digest='d3')

    vc1 = ViewChange(viewNo=0, stableCheckpoint=0,
                     prepared=[(1, 1, "digest1")],
                     preprepared=[(1, 1, "digest1")],
                     checkpoints=[cp1])

    vc2_not_stable = ViewChange(viewNo=0, stableCheckpoint=0,
                                prepared=[(1, 1, "digest1")],
                                preprepared=[(1, 1, "digest1")],
                                checkpoints=[cp2])

    vc2_stable = ViewChange(viewNo=0, stableCheckpoint=10,
                            prepared=[(1, 1, "digest1")],
                            preprepared=[(1, 1, "digest1")],
                            checkpoints=[cp2])

    vc3_not_stable = ViewChange(viewNo=0, stableCheckpoint=10,
                                prepared=[(1, 1, "digest1")],
                                preprepared=[(1, 1, "digest1")],
                                checkpoints=[cp3])

    vc3_stable = ViewChange(viewNo=0, stableCheckpoint=10,
                            prepared=[(1, 1, "digest1")],
                            preprepared=[(1, 1, "digest1")],
                            checkpoints=[cp3])

    for vc3 in (vc3_not_stable, vc3_stable):
        for vc2 in (vc2_not_stable, vc2_stable):
            vcs = [vc1, vc2, vc3, vc3]
            assert builder.calc_checkpoint(vcs) == cp3

            vcs = [vc1, vc3, vc3, vc3]
            assert builder.calc_checkpoint(vcs) == cp3

            vcs = [vc2, vc3, vc3, vc3]
            assert builder.calc_checkpoint(vcs) == cp3

            vcs = [vc2, vc2, vc3, vc3]
            assert builder.calc_checkpoint(vcs) == cp3

            vcs = [vc1, vc1, vc3, vc3]
            assert builder.calc_checkpoint(vcs) == cp3

            vcs = [vc1, vc1, vc1, vc3]
            assert builder.calc_checkpoint(vcs) == cp1

            vcs = [vc1, vc1, vc2, vc2]
            assert builder.calc_checkpoint(vcs) == cp2

            vcs = [vc2, vc2, vc2, vc3]
            assert builder.calc_checkpoint(vcs) == cp2


def test_calc_checkpoints_digest(builder):
    cp1_d1 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='d1')
    cp2_d2 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='d2')
    cp2_d1 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='d1')

    vc1_d1 = ViewChange(viewNo=0, stableCheckpoint=0,
                        prepared=[(1, 1, "digest1")],
                        preprepared=[(1, 1, "digest1")],
                        checkpoints=[cp1_d1])
    vc2_d2 = ViewChange(viewNo=0, stableCheckpoint=0,
                        prepared=[(1, 1, "digest1")],
                        preprepared=[(1, 1, "digest1")],
                        checkpoints=[cp2_d2])
    vc2_d1 = ViewChange(viewNo=0, stableCheckpoint=0,
                        prepared=[(1, 1, "digest1")],
                        preprepared=[(1, 1, "digest1")],
                        checkpoints=[cp2_d1])

    vcs = [vc1_d1, vc1_d1, vc2_d1, vc2_d2]
    assert builder.calc_checkpoint(vcs) == cp1_d1

    vcs = [vc1_d1, vc2_d1, vc2_d2, vc2_d2]
    assert builder.calc_checkpoint(vcs) == cp2_d2

    vcs = [vc1_d1, vc2_d2, vc2_d1, vc2_d1]
    assert builder.calc_checkpoint(vcs) == cp2_d1

    # Here we have 2 nodes malicious (f=1), but calc_checkpoint returns a value depending on the order
    # Is it OK, or calc_checkpoint should return None (indicating that there is no valid quorum)?
    vcs = [vc2_d1, vc2_d1, vc2_d2, vc2_d2]
    assert builder.calc_checkpoint(vcs) == cp2_d1
    vcs = [vc2_d2, vc2_d2, vc2_d1, vc2_d1]
    assert builder.calc_checkpoint(vcs) == cp2_d2


# @pytest.fixture(params=range(2))
# def random(request):
#     return DefaultSimRandom(request.param)


def test_calc_batches_combinations(builder, random):
    MAX_PP_SEQ_NO = 20
    CHEQ_FREQ = 10
    MAX_VIEW_NO = 3
    MAX_DIGEST_ID = 20

    cp1 = Checkpoint(instId=0, viewNo=1, seqNoStart=0, seqNoEnd=0, digest='d1')
    cp2 = Checkpoint(instId=0, viewNo=1, seqNoStart=0, seqNoEnd=CHEQ_FREQ, digest='d2')

    for i in range(100):
        for vc_count in range(N - F, N + 1):
            view_changes = []

            # 1. INIT
            for i in range(vc_count):
                # PRE-PREPARED
                num_preprepares = random.integer(0, MAX_PP_SEQ_NO)
                pre_prepares = [(random.integer(0, MAX_VIEW_NO), i, "digest{}".format(random.integer(1, MAX_DIGEST_ID)))
                                for i in range(1, num_preprepares + 1)]

                # PREPARED
                prepares_mode = random.sample(['all-preprepared', 'half-preprepared', 'random-preprepared', 'random'],
                                              1)
                if prepares_mode == ['all-preprepared']:
                    prepares = pre_prepares
                elif prepares_mode == ['half-preprepared']:
                    prepares = pre_prepares[:(num_preprepares // 2)]
                elif prepares_mode == ['random-preprepared']:
                    prepares = random.sample(pre_prepares, len(pre_prepares))
                elif prepares_mode == ['random']:
                    num_prepares = random.integer(0, MAX_PP_SEQ_NO)
                    prepares = [
                        (random.integer(0, MAX_VIEW_NO), i, "digest{}".format(random.integer(1, MAX_DIGEST_ID)))
                        for i in range(1, num_prepares + 1)]
                else:
                    assert False, str(prepares_mode)

                # CHECKPOINTS
                checkpoints = random.sample([cp1, cp2], 1)
                stable_checkpoint = random.sample([0, CHEQ_FREQ], 1)[0]
                view_changes.append(ViewChange(
                    viewNo=MAX_VIEW_NO,
                    stableCheckpoint=stable_checkpoint,
                    prepared=prepares,
                    preprepared=pre_prepares,
                    checkpoints=checkpoints
                ))

            # 2. EXECUTE
            batches1 = builder.calc_batches(cp1, list(view_changes))
            batches2 = builder.calc_batches(cp2, list(view_changes))

            # 3. VALIDATE
            committed = calc_committed(view_changes, MAX_PP_SEQ_NO, N, F)
            if committed and batches1:
                assert committed == batches1[:len(committed)]
                committed = [c for c in committed if c.pp_seq_no > cp2.seqNoEnd]
                assert committed == batches2[:len(committed)]

import pytest

from plenum.common.messages.node_messages import PrePrepare, Checkpoint
from plenum.test.consensus.helper import SimPool
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom


def some_preprepare(random: SimRandom, view_no: int, pp_seq_no: int) -> PrePrepare:
    return PrePrepare(
        instId=0, viewNo=view_no, ppSeqNo=pp_seq_no, ppTime=1499906903,
        reqIdr=[], discarded="", digest=random.string(40),
        ledgerId=1, stateRootHash=None, txnRootHash=None,
        sub_seq_no=0, final=True
    )


def some_checkpoint(random: SimRandom, view_no: int, pp_seq_no: int) -> Checkpoint:
    return Checkpoint(
        instId=0, viewNo=view_no, seqNoStart=pp_seq_no, seqNoEnd=pp_seq_no, digest=random.string(40)
    )


def check_view_change_completes_under_normal_conditions(random: SimRandom):
    pool_size = random.integer(4, 8)
    pool = SimPool(pool_size, random)

    # Create simulated history
    # TODO: Move into helper?
    faulty = (pool_size - 1) // 3
    seq_no_per_cp = 10
    max_batches = 50
    batches = [some_preprepare(random, 0, n) for n in range(1, max_batches)]
    checkpoints = [some_checkpoint(random, 0, n) for n in range(0, max_batches, seq_no_per_cp)]

    # Preprepares
    pp_count = [random.integer(0, len(batches)) for _ in range(pool_size)]
    max_pp = sorted(pp_count)[faulty]
    # Prepares
    p_count = [random.integer(0, min(max_pp, pp)) for pp in pp_count]
    max_p = sorted(p_count)[faulty]
    # Checkpoints
    cp_count = [1 + random.integer(0, min(max_p, p)) // seq_no_per_cp for p in pp_count]
    max_stable_cp = sorted(cp_count)[faulty] - 1
    stable_cp = [checkpoints[random.integer(0, min(max_stable_cp, cp))].seqNoEnd for cp in cp_count]

    # Initialize consensus data
    for i, node in enumerate(pool.nodes):
        node._data.preprepared = batches[:pp_count[i]]
        node._data.prepared = batches[:p_count[i]]
        node._data.checkpoints = checkpoints[:cp_count[i]]
        node._data.stable_checkpoint = stable_cp[i]

    # Schedule view change at different time on all nodes
    for node in pool.nodes:
        pool.timer.schedule(random.integer(0, 10000),
                            node._view_changer.start_view_change)

    # Make sure all nodes complete view change
    pool.timer.wait_for(lambda: all(not node._data.waiting_for_new_view
                                    and node._data.view_no > 0
                                    for node in pool.nodes))

    # Make sure all nodes end up in same state
    for node_a, node_b in zip(pool.nodes, pool.nodes[1:]):
        assert node_a._data.view_no == node_b._data.view_no
        assert node_a._data.primary_name == node_b._data.primary_name
        assert node_a._data.stable_checkpoint == node_b._data.stable_checkpoint
        assert node_a._data.preprepared == node_b._data.preprepared


@pytest.mark.parametrize("seed", range(1000))
def test_view_change_completes_under_normal_conditions(seed):
    random = DefaultSimRandom(seed)
    check_view_change_completes_under_normal_conditions(random)

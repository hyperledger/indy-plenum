import pytest

from plenum.server.consensus.view_change_service import BatchID
from plenum.test.consensus.view_change.helper import some_pool
from plenum.test.helper import MockNetwork
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom


def check_view_change_completes_under_normal_conditions(random: SimRandom):
    # Create random pool with random initial state
    pool, committed = some_pool(random)

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

    # Make sure that all committed reqs are ordered with the same ppSeqNo in the new view:
    stable_checkpoint = pool.nodes[0]._data.stable_checkpoint
    committed = [c for c in committed if c.pp_seq_no > stable_checkpoint]
    for n in pool.nodes:
        assert committed == n._data.preprepared[:len(committed)]


def calc_committed(view_changes):
    committed = []
    for pp_seq_no in range(1, 50):
        batch_id = None
        for vc in view_changes:
            # pp_seq_no must be present in all PrePrepares
            for pp in vc.preprepared:
                if pp[1] == pp_seq_no:
                    if batch_id is None:
                        batch_id = pp
                    assert batch_id == pp
                    break

            # pp_seq_no must be present in all Prepares
            if batch_id not in vc.prepared:
                return committed
        committed.append(BatchID(*batch_id))
    return committed


@pytest.mark.parametrize("seed", range(1000))
def test_view_change_completes_under_normal_conditions(seed):
    random = DefaultSimRandom(seed)
    check_view_change_completes_under_normal_conditions(random)


def test_new_view_combinations(random):
    # Create pool in some random initial state
    pool, _ = some_pool(random)
    quorums = pool.nodes[0]._data.quorums

    # Get view change votes from all nodes
    view_change_messages = []
    for node in pool.nodes:
        network = MockNetwork()
        node._view_changer._network = network
        node._view_changer.start_view_change()
        view_change_messages.append(network.sent_messages[0][0])

    # Check that all committed requests are present in final batches
    for _ in range(10):
        num_votes = quorums.strong.value
        votes = random.sample(view_change_messages, num_votes)

        cp = pool.nodes[0]._view_changer._new_view_builder.calc_checkpoint(votes)
        assert cp is not None

        batches = pool.nodes[0]._view_changer._new_view_builder.calc_batches(cp, votes)
        committed = calc_committed(votes)
        committed = [c for c in committed if c.pp_seq_no > cp.seqNoEnd]

        assert batches is not None
        assert committed == batches[:len(committed)]

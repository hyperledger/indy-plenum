from collections import Counter
from functools import partial

import pytest

from plenum.common.messages.internal_messages import NeedViewChange
from plenum.common.messages.node_messages import ViewChange, NewView
from plenum.server.consensus.batch_id import BatchID
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.test.consensus.view_change.helper import some_pool
from plenum.test.helper import MockNetwork
from plenum.test.simulation.sim_network import Processor, Discard, message_dst, message_type
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom


@pytest.fixture(params=[(0, 0.6)])
def latency(request, tconf):
    min_latency, max_latency = tuple(int(param * tconf.NEW_VIEW_TIMEOUT) for param in request.param)
    return min_latency, max_latency


@pytest.fixture(params=[
    # ([ViewChange, NewView, ViewChangeAck], 0.02),
    ([ViewChange], 1),
    ([NewView], 1),
    ])
def filter(request):
    return request.param[0], request.param[1]


def test_view_change_completes_under_normal_conditions_default_seeds(random, latency, filter):
    check_view_change_completes_under_normal_conditions(random, *latency, *filter)


@pytest.mark.parametrize(argnames="seed", argvalues=[290370, 749952, 348636, 919685, 674863, 378187])
def test_view_change_completes_under_normal_conditions_regression_seeds(seed, latency, filter):
    random = DefaultSimRandom(seed)
    check_view_change_completes_under_normal_conditions(random, *latency, *filter)


def test_view_change_permutations(random):
    # Create pool in some random initial state
    pool, _ = some_pool(random)
    quorums = pool.nodes[0]._data.quorums

    # Get view change votes from all nodes
    view_change_messages = []
    for node in pool.nodes:
        network = MockNetwork()
        node._view_changer._network = network
        node._view_changer._bus.send(NeedViewChange())
        view_change_messages.append(network.sent_messages[0][0])

    # Select random number of view change votes
    num_view_changes = random.integer(quorums.view_change.value, quorums.n)
    view_change_messages = random.sample(view_change_messages, num_view_changes)

    # Check that all committed requests are present in final batches
    new_view_builder = pool.nodes[0]._view_changer._new_view_builder
    cps = {new_view_builder.calc_checkpoint(random.shuffle(view_change_messages))
           for _ in range(10)}
    assert len(cps) == 1


# ToDo: this test fails on seeds {440868, 925547}
def test_new_view_combinations(random):
    # Create pool in some random initial state
    pool, _ = some_pool(random)
    quorums = pool.nodes[0]._data.quorums

    # Get view change votes from all nodes
    view_change_messages = []
    for node in pool.nodes:
        network = MockNetwork()
        node._view_changer._network = network
        node._view_changer._bus.send(NeedViewChange())
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


def check_view_change_completes_under_normal_conditions(random: SimRandom,
                                                        min_latency, max_latency,
                                                        filtered_msg_types, filter_probability):
    # PREPARE

    # 1. Create random pool with random initial state
    pool, committed = some_pool(random)
    N = pool.size
    F = (N - 1) // 3
    initial_view_no = pool._initial_view_no

    # 2. set latency
    pool.network.set_latency(min_latency, max_latency)

    # 3. set filters
    pool.network.add_processor(Discard(probability=filter_probability),
                               message_type(filtered_msg_types),
                               message_dst(replica_name_to_node_name(pool.nodes[-1].name)))
    # EXECUTE

    # Schedule view change at different time on all nodes
    for node in pool.nodes:
        pool.timer.schedule(random.float(0, 10),
                            partial(node._view_changer.process_need_view_change, NeedViewChange()))

    # CHECK

    # 1. Make sure all nodes complete view change
    pool.timer.wait_for(lambda: all(not node._data.waiting_for_new_view
                                    and node._data.view_no > initial_view_no
                                    for node in pool.nodes))

    # 2. check that equal stable checkpoint is set on at least N-F nodes (F nodes may lag behind and will catchup)
    stable_checkpoints = [n._data.stable_checkpoint for n in pool.nodes]
    most_freq_stable_ckeckpoint = Counter(stable_checkpoints).most_common(1)
    stable_checkpoint = most_freq_stable_ckeckpoint[0][0]
    assert most_freq_stable_ckeckpoint[0][1] >= N - F

    # 3. check that equal preprepares is set on all node with the found stable checkpoint
    preprepares = set()
    for n in pool.nodes:
        if n._data.stable_checkpoint >= stable_checkpoint:
            preprepares.add(tuple(n._data.preprepared))
    assert len(preprepares) == 1

    # 4. Make sure all nodes end up in same view
    for node_a, node_b in zip(pool.nodes, pool.nodes[1:]):
        assert node_a._data.view_no == node_b._data.view_no
        assert node_a._data.primary_name == node_b._data.primary_name

    # 5. Make sure that all committed reqs are ordered with the same ppSeqNo in the new view:
    committed_above_cp = [c for c in committed if c.pp_seq_no > stable_checkpoint]
    for n in pool.nodes:
        if n._data.stable_checkpoint >= stable_checkpoint:
            for expected_batch, actual_batch in zip(committed_above_cp, n._data.preprepared[:len(committed_above_cp)]):
                assert expected_batch.pp_view_no == actual_batch.pp_view_no
                assert expected_batch.pp_seq_no == actual_batch.pp_seq_no
                assert expected_batch.pp_digest == actual_batch.pp_digest


def calc_committed(view_changes):
    committed = []
    for pp_seq_no in range(1, 50):
        batch_id = None
        for vc in view_changes:
            # pp_seq_no must be present in all PrePrepares
            for pp in vc.preprepared:
                if pp[2] == pp_seq_no:
                    if batch_id is None:
                        batch_id = pp
                    assert batch_id == pp
                    break

            # pp_seq_no must be present in all Prepares
            if batch_id not in vc.prepared:
                return committed
        committed.append(BatchID(*batch_id))
    return committed

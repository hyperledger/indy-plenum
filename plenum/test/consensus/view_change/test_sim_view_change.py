from collections import Counter
from functools import partial
from random import Random

import pytest

from plenum.common.messages.internal_messages import NeedViewChange
from plenum.common.messages.node_messages import ViewChange
from plenum.server.consensus.batch_id import BatchID
from plenum.server.replica_helper import getNodeName
from plenum.test.consensus.view_change.helper import some_pool
from plenum.test.helper import MockNetwork
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom


@pytest.fixture(params=[(0, 0.6), (1, 2)])
def latency(request, tconf):
    min_latency, max_latency = tuple(int(param * tconf.NEW_VIEW_TIMEOUT) for param in request.param)
    return min_latency, max_latency


@pytest.fixture(params=[
    # ([ViewChange, NewView, ViewChangeAck], 0.02),
    ([ViewChange], 1)])
def filter(request):
    return request.param[0], request.param[1]


@pytest.fixture(params=range(150, 200))
def default_random(request):
    seed = request.param
    return DefaultSimRandom(seed)


@pytest.fixture(params=Random().sample(range(1000000), 100))
def random_random(request):
    seed = request.param
    return DefaultSimRandom(seed)


def test_view_change_completes_under_normal_conditions_default_seeds(default_random, latency, filter):
    check_view_change_completes_under_normal_conditions(default_random, *latency, *filter)


def test_view_change_completes_under_normal_conditions_random_seeds(random_random, latency, filter):
    check_view_change_completes_under_normal_conditions(random_random, *latency, *filter)


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

    # 2. set latency
    pool.network.set_latency(min_latency, max_latency)

    # 3. set filter
    pool.network.set_filter([getNodeName(pool.nodes[-1].name)],
                            filtered_msg_types, filter_probability)

    # EXECUTE

    # Schedule view change at different time on all nodes
    for node in pool.nodes:
        pool.timer.schedule(random.integer(0, 10000),
                            partial(node._view_changer.process_need_view_change, NeedViewChange()))

    # CHECK

    # 1. Make sure all nodes complete view change
    pool.timer.wait_for(lambda: all(not node._data.waiting_for_new_view
                                    and node._data.view_no > 0
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
            assert committed_above_cp == n._data.preprepared[:len(committed_above_cp)]


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

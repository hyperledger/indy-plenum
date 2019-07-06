import pytest

from plenum.test.consensus.helper import SimPool
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom


def check_view_change_completes_under_normal_conditions(random: SimRandom):
    pool_size = random.integer(4, 8)
    pool = SimPool(pool_size, random)

    view_nos = {node._data.view_no for node in pool.nodes}
    assert len(view_nos) == 1
    initial_view_no = view_nos.pop()

    # Schedule view change at different time on all nodes
    for node in pool.nodes:
        pool.timer.schedule(random.integer(0, 1),
                            node._view_changer.start_view_change)

    # Make sure all nodes complete view change
    pool.timer.wait_for(lambda: all(not node._data.waiting_for_new_view
                                    and node._data.view_no > initial_view_no
                                    for node in pool.nodes))

    # Make sure all nodes end up in same state
    for node_a, node_b in zip(pool.nodes, pool.nodes[1:]):
        assert node_a._data.view_no == node_b._data.view_no
        assert node_a._data.primary_name == node_b._data.primary_name
        assert node_a._data.preprepared == node_b._data.preprepared


@pytest.mark.parametrize("seed", range(1000))
def test_view_change_completes_under_normal_conditions(seed):
    random = DefaultSimRandom(seed)
    check_view_change_completes_under_normal_conditions(random)

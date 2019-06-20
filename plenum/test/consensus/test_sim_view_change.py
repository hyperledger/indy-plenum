import pytest

from plenum.test.consensus.helper import SimPool
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom


def check_view_change_completes_under_normal_conditions(random: SimRandom):
    pool = SimPool(4, random)
    for node in pool.nodes:
        node._view_changer.start_view_change()

    for node in pool.nodes:
        pool.timer.wait_for(lambda: not node._data.waiting_for_new_view)


@pytest.mark.parametrize("seed", range(50))
def test_view_change_completes_under_normal_conditions(seed):
    random = DefaultSimRandom(seed)
    check_view_change_completes_under_normal_conditions(random)

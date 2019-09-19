from functools import partial

import pytest

from plenum.common.messages.node_messages import ViewChange

from plenum.common.messages.internal_messages import NeedViewChange
from plenum.server.consensus.batch_id import BatchID
from plenum.server.replica_helper import getNodeName
from plenum.test.consensus.view_change.helper import some_pool
from plenum.test.helper import MockNetwork
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom
from plenum.test.stasher import delay_rules


def check_view_change_completes_under_normal_conditions(random: SimRandom):
    # Create random pool with random initial state
    pool, committed = some_pool(random)

    pool.network.set_filter([getNodeName(pool.nodes[-1].name)],
                            [ViewChange])
    # Schedule view change at different time on all nodes
    for node in pool.nodes:
        pool.timer.schedule(random.integer(0, 10000),
                            partial(node._view_changer.process_need_view_change, NeedViewChange()))

    # Make sure all nodes complete view change
    pool.timer.wait_for(lambda: all(not node._data.waiting_for_new_view
                                    and node._data.view_no > 0
                                    for node in pool.nodes))
    pool.network.reset_filters()

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


@pytest.mark.skip
@pytest.mark.parametrize("seed", range(150))
def test_view_change_with_delays(seed):
    random = DefaultSimRandom(seed)
    check_view_change_completes_under_normal_conditions(random)
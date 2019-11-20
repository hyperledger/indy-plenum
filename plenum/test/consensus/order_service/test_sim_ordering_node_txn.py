from functools import partial
import random

import pytest

from plenum.common.constants import VALIDATOR, POOL_LEDGER_ID
from plenum.common.timer import RepeatingTimer
from plenum.test.consensus.order_service.sim_helper import order_requests, \
    get_pools_ledger_size, setup_pool, check_batch_count
from plenum.test.greek import Greeks
from plenum.test.node_reg_handler.test_node_reg_handler import build_node_req
from plenum.test.simulation.sim_random import DefaultSimRandom


MAX_BATCH_SIZE = 1


@pytest.fixture(params=['promote'])
def req(request):
    def wrap(pool_size):
        if request.param == 'promote':
            return build_node_req(Greeks[pool_size][0], [VALIDATOR])
        else:
            node_index = random.randint(0, pool_size - 1)
            return build_node_req(Greeks[node_index][0], [])
    return wrap


def test_node_txn_add_new_node(req):
    pool = setup_pool(DefaultSimRandom())
    req = req(pool.size)
    pool.sim_send_requests([req])
    initial_ledger_size = get_pools_ledger_size(pool, ledger_id=POOL_LEDGER_ID)

    random_interval = 1000
    RepeatingTimer(pool.timer, random_interval, partial(order_requests, pool))

    for node in pool.nodes[:-1]:
        pool.timer.wait_for(
            partial(
                check_batch_count, node, 1))
    assert pool.size > initial_ledger_size

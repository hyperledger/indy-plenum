from functools import partial
from random import Random

import pytest

from plenum.common.messages.internal_messages import NeedViewChange
from plenum.common.timer import RepeatingTimer
from plenum.test.consensus.order_service.sim_helper import MAX_BATCH_SIZE, setup_pool, order_requests, \
    check_consistency, check_batch_count, check_ledger_size, get_pools_ledger_size, create_requests
from plenum.test.simulation.sim_random import DefaultSimRandom

REQUEST_COUNT = 10


def test_view_change_while_ordering_with_real_msgs_random_seed(random):
    do_test(random)


@pytest.mark.parametrize("seed", [3957])
def test_view_change_while_ordering_with_real_msgs_fixed_seed(seed):
    do_test(DefaultSimRandom(seed))


def do_test(random):
    # 1. Setup pool
    requests_count = REQUEST_COUNT
    batches_count = requests_count // MAX_BATCH_SIZE
    reqs = create_requests(requests_count)
    pool = setup_pool(random)
    initial_view_no = pool._initial_view_no
    pool.sim_send_requests(reqs)
    initial_ledger_size = get_pools_ledger_size(pool)

    # 2. Send 3pc batches
    random_interval = 1
    RepeatingTimer(pool.timer, random_interval, partial(order_requests, pool))

    for node in pool.nodes:
        pool.timer.schedule(3, partial(node._view_changer.process_need_view_change, NeedViewChange()))
    # 3. Make sure that view_change is completed
    pool.timer.wait_for(lambda: all(not node._data.waiting_for_new_view
                                    and node._data.view_no > initial_view_no
                                    for node in pool.nodes))

    # 3. Make sure all nodes ordered all the requests
    for node in pool.nodes:
        pool.timer.wait_for(partial(check_batch_count, node, batches_count))
        pool.timer.wait_for(partial(check_ledger_size, node, initial_ledger_size + REQUEST_COUNT))

    # 4. Check data consistency
    pool.timer.wait_for(lambda: check_no_asserts(check_consistency, pool))


# TODO: Either move into helper or start changing existing assertion handling
def check_no_asserts(func, *args):
    try:
        func(*args)
    except AssertionError:
        return False
    return True

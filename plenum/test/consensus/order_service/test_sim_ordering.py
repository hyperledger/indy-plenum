from functools import partial
from random import Random

import pytest

from plenum.common.timer import RepeatingTimer
from plenum.test.consensus.order_service.sim_helper import MAX_BATCH_SIZE, setup_pool, order_requests, \
    check_batch_count, check_consistency, create_requests
from plenum.test.simulation.sim_random import DefaultSimRandom

REQUEST_COUNT = 10


@pytest.mark.parametrize("seed", range(100))
def test_ordering_with_real_msgs_default_seed(seed):
    do_test(seed)


@pytest.mark.parametrize("seed", Random().sample(range(1000000), 100))
def test_ordering_with_real_msgs_random_seed(seed):
    do_test(seed)


def do_test(seed):
    # 1. Setup pool
    requests_count = REQUEST_COUNT
    batches_count = requests_count // MAX_BATCH_SIZE
    random = DefaultSimRandom(seed)
    reqs = create_requests(requests_count)
    pool = setup_pool(random)
    pool.sim_send_requests(reqs)

    # 2. Send 3pc batches
    random_interval = 1
    RepeatingTimer(pool.timer, random_interval, partial(order_requests, pool))

    # 3. Make sure all nodes ordered all the requests
    for node in pool.nodes:
        pool.timer.wait_for(partial(check_batch_count, node, batches_count))

    # 4. Check data consistency
    check_consistency(pool)

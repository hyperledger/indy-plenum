from random import Random

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.consensus.order_service.sim_helper import create_requests, setup_pool
from plenum.test.simulation.sim_random import DefaultSimRandom

REQS_COUNT = 10


# "params" equal to seed
@pytest.fixture(params=Random().sample(range(1000000), 100))
def random(request):
    return DefaultSimRandom(request.param)


@pytest.fixture()
def requests(random):
    return create_requests(random.integer(5, REQS_COUNT))


@pytest.fixture()
def pool(random):
    return setup_pool(random)


def test_get_requestQueues_back_after_revert(pool, requests):

    def mock_batch_sending(node):
        node._orderer.send_pre_prepare = lambda *args, **kwargs: True

    pool.sim_send_requests(requests)
    primary = [n for n in pool.nodes if n._data.is_primary][0]
    queue_before = primary._orderer.requestQueues[DOMAIN_LEDGER_ID]
    mock_batch_sending(primary)
    while not primary._orderer.requestQueues:
        primary.send_3pc_batch()

    # imitate view_change starting
    primary._orderer.revert_unordered_batches()
    assert primary._orderer.requestQueues[DOMAIN_LEDGER_ID]
    assert primary._orderer.requestQueues[DOMAIN_LEDGER_ID] == queue_before
    for node in pool.nodes:
        assert primary._orderer.requestQueues[DOMAIN_LEDGER_ID] == node._orderer.requestQueues[DOMAIN_LEDGER_ID]

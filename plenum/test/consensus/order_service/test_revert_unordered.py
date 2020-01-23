import copy

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.consensus.order_service.sim_helper import create_requests, setup_pool, MAX_BATCH_SIZE, update_config

REQS_COUNT = 10


@pytest.fixture()
def requests(random):
    return create_requests(MAX_BATCH_SIZE * random.integer(5, REQS_COUNT))


@pytest.fixture()
def pool(random):
    def mock_batch_sending(node):
        node._orderer.send_pre_prepare = lambda *args, **kwargs: True

    pool = setup_pool(random, {'Max3PCBatchesInFlight': 100})
    for node in pool.nodes:
        mock_batch_sending(node)
    return pool


def test_get_requestQueues_back_after_revert(pool, requests):
    pool.sim_send_requests(requests)
    primary = [n for n in pool.nodes if n._data.is_primary][0]
    primary._orderer.last_ordered_3pc = (primary._orderer.last_ordered_3pc[0], 1)
    queue_before = copy.copy(primary._orderer.requestQueues[DOMAIN_LEDGER_ID])
    assert primary._orderer.can_send_3pc_batch()
    while primary._orderer.requestQueues[DOMAIN_LEDGER_ID]:
        primary._orderer.send_3pc_batch()

    # imitate view_change starting
    assert len(primary._orderer.batches) > 1
    primary._orderer.revert_unordered_batches()
    assert primary._orderer.requestQueues[DOMAIN_LEDGER_ID]
    assert len(primary._orderer.requestQueues[DOMAIN_LEDGER_ID].difference(queue_before)) == 0
    for node in pool.nodes:
        assert len(primary._orderer.requestQueues[DOMAIN_LEDGER_ID].difference(node._orderer.requestQueues[DOMAIN_LEDGER_ID])) == 0


def test_the_same_order_inside_batch_after_revert(pool, requests):

    pool.sim_send_requests(requests)
    primary = [n for n in pool.nodes if n._data.is_primary][0]
    queue_before = copy.copy(primary._orderer.requestQueues[DOMAIN_LEDGER_ID])
    update_config(primary._orderer._config, {'Max3PCBatchSize': len(requests)})
    while primary._orderer.requestQueues[DOMAIN_LEDGER_ID]:
        primary._orderer.send_3pc_batch()

    # imitate view_change starting
    assert len(primary._orderer.batches) == 1
    primary._orderer.revert_unordered_batches()
    assert primary._orderer.requestQueues[DOMAIN_LEDGER_ID]
    assert primary._orderer.requestQueues[DOMAIN_LEDGER_ID] == queue_before
    for node in pool.nodes:
        assert primary._orderer.requestQueues[DOMAIN_LEDGER_ID] == node._orderer.requestQueues[DOMAIN_LEDGER_ID]

import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.request import ReqKey
from plenum.common.startable import Mode
from plenum.server.replica_helper import getNodeName
from plenum.test.consensus.helper import SimPool
from plenum.test.helper import sdk_random_request_objects
from plenum.test.simulation.sim_random import DefaultSimRandom
from plenum.test.testing_utils import FakeSomething

MAX_BATCH_SIZE = 2
REQUEST_COUNT = 10


def create_requests(count):
    return sdk_random_request_objects(count, CURRENT_PROTOCOL_VERSION)


def create_pool(random):
    pool_size = random.integer(4, 8)
    pool = SimPool(pool_size, random)
    return pool


def sim_send_requests(pool: SimPool, req_count):
    reqs = create_requests(req_count)
    faulty = (pool.size - 1) // 3
    for node in pool.nodes:
        for req in reqs:
            node._data.requests.add(req)
            node._data.requests.mark_as_forwarded(req, faulty + 1)
            node._data.requests.set_finalised(req)
            node.ready_for_3pc(ReqKey(req.key))


def setup_consensus_data(cd):
    cd.node_mode = Mode.participating


def setup_pool(random, req_count):
    pool = create_pool(random)

    for node in pool.nodes:
        node._orderer._config.Max3PCBatchSize = MAX_BATCH_SIZE
        node._orderer._config.CHK_FREQ = 5
        node._orderer._config.LOG_SIZE = 3 * node._orderer._config.CHK_FREQ
        setup_consensus_data(node._data)
        node._write_manager.future_primary_handler = FakeSomething(get_last_primaries=lambda: node._data.primaries)
        node._orderer._network._connecteds = list(set(node._data.validators) - {getNodeName(node._data.name)})

    sim_send_requests(pool, req_count)

    return pool


def check_consistency(pool):
    for node in pool.nodes:
        for another_node in pool.nodes:
            for ledger_id in node._write_manager.ledger_ids:
                state = node._write_manager.database_manager.get_state(ledger_id)
                if state:
                    assert state.headHash == \
                           another_node._write_manager.database_manager.get_state(ledger_id).headHash
                assert node._write_manager.database_manager.get_ledger(ledger_id).uncommittedRootHash == \
                    another_node._write_manager.database_manager.get_ledger(ledger_id).uncommittedRootHash


@pytest.mark.parametrize("seed", range(10))
def test_ordering_with_real_msgs(seed):
    # 1. Setup pool
    requests_count = REQUEST_COUNT
    batches_count = requests_count // MAX_BATCH_SIZE
    random = DefaultSimRandom(seed)
    pool = setup_pool(random, requests_count)
    primary_node = [n for n in pool.nodes if n._data.is_primary][0]

    # 2. Send 3pc batches
    prev_t = 0
    for i in range(batches_count):
        prev_t += random.integer(0, 10000)
        pool.timer.schedule(prev_t,
                            primary_node._orderer.send_3pc_batch)
    # for node in pool.nodes:
    #     pool.timer.schedule(random.integer(1000, 10000),
    #                         partial(node._view_changer.process_need_view_change, NeedViewChange()))

    # 3. Make sure all nodes ordered all the requests
    for node in pool.nodes:
        pool.timer.wait_for(lambda: node._orderer.last_ordered_3pc[1] == batches_count, timeout=200000)

    # 4. Check data consistency
    check_consistency(pool)

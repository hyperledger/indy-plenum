from functools import partial
from random import randint, shuffle, Random

import pytest

from plenum.common.constants import VALIDATOR, POOL_LEDGER_ID, TYPE, NODE, DATA, ALIAS, CLIENT_IP, CLIENT_PORT, NODE_IP, \
    NODE_PORT, BLS_KEY, TARGET_NYM, SERVICES, CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID
from plenum.common.messages.internal_messages import NeedViewChange
from plenum.common.request import Request
from plenum.common.timer import RepeatingTimer
from plenum.common.txn_util import get_payload_data, get_from
from plenum.common.util import getMaxFailures, randomString
from plenum.server.consensus.primary_selector import RoundRobinNodeRegPrimariesSelector, \
    RoundRobinConstantNodesPrimariesSelector
from plenum.server.consensus.replica_service import ReplicaService
from plenum.test.consensus.helper import SimPool
from plenum.test.consensus.order_service.sim_helper import order_requests, \
    get_pools_ledger_size, setup_pool, check_batch_count, check_ledger_size, create_requests
from plenum.test.greek import Greeks
from plenum.test.simulation.sim_random import DefaultSimRandom
from stp_core.common.log import Logger, getlogger


logger = getlogger()


DOMAIN_REQ_COUNT = 10


def build_add_new_node_req(node_name, services, ports_addiction):
    operation = {
        TYPE: NODE,
        DATA: {
            ALIAS: node_name,
            CLIENT_IP: '127.0.0.1',
            CLIENT_PORT: 7000 + ports_addiction,
            NODE_IP: '127.0.0.1',
            NODE_PORT: 7002 + ports_addiction,
            BLS_KEY: '00000000000000000000000000000000',
        },
        TARGET_NYM: node_name
    }
    if services is not None:
        operation[DATA][SERVICES] = services

    return Request(operation=operation, reqId=1513945121191691,
                   protocolVersion=CURRENT_PROTOCOL_VERSION, identifier=randomString())


def build_demote_node_req(node_name, txn):
    txn_data = get_payload_data(txn)
    target_nym = txn_data[TARGET_NYM]
    txn_data = txn_data['data']
    operation = {
        TYPE: NODE,
        DATA: {
            ALIAS: node_name,
            CLIENT_IP: txn_data[CLIENT_IP],
            CLIENT_PORT: txn_data[CLIENT_PORT],
            NODE_IP: txn_data[NODE_IP],
            NODE_PORT: txn_data[NODE_PORT],
            SERVICES: [],
        },
        TARGET_NYM: target_nym
    }
    return Request(operation=operation, reqId=1513945121191691,
                   protocolVersion=CURRENT_PROTOCOL_VERSION, identifier=get_from(txn))


def check_primaries(pool: SimPool, expected_primaries):
    for node in pool.nodes:
        if node._data.primaries != expected_primaries:
            logger.debug("Node: {}, current primaries: {}, expected_primaries: {}".format(
                node.name, node._data.primaries, expected_primaries
            ))
            return False
    return True


def check_node_reg(pool: SimPool, expected_node_reg):
    for node in pool.nodes:
        committed_node_reg = node._write_manager.node_reg_handler.committed_node_reg
        if set(committed_node_reg).difference(set(expected_node_reg)):
            logger.debug("Node: {}, Current node reg: {}, Expected node reg: {}".format(
                node.name,
                committed_node_reg,
                expected_node_reg
            ))
            return False
    return True


def get_txn_data_from_ledger(ledger, alias):
    for _, t in ledger.getAllTxn():
        txn_data = get_payload_data(t)['data']
        if txn_data[ALIAS] == alias:
            return t


@pytest.fixture()
def node_req_add(random, sim_pool):
    def wrap(pool_size):
        return build_add_new_node_req(Greeks[pool_size][0], [VALIDATOR], pool_size)
    return wrap


@pytest.fixture()
def node_req_demote(random, sim_pool):
    def wrap(node_index):
        node_to_demote = sim_pool.nodes[node_index]
        return node_to_demote.name.split(':')[0], \
               build_demote_node_req(Greeks[node_index][0],
                                     get_txn_data_from_ledger(
                                         node_to_demote._write_manager.database_manager.get_ledger(POOL_LEDGER_ID),
                                         Greeks[node_index][0]))
    return wrap


# "params" equal to seed
@pytest.fixture(params=Random().sample(range(1000000), 100))
def random(request):
    seed = request.param
    # TODO: Remove after we fix INDY-2237 and INDY-2148
    if seed in {752248, 659043, 550513, 141156}:
        return DefaultSimRandom(0)
    return DefaultSimRandom(request.param)


@pytest.fixture()
def sim_pool(random):
    return setup_pool(random)


# Get list of random indexes. Count of this indexes is not greater then
@pytest.fixture()
def indexes_to_demote(sim_pool, random):
    return random.sample(range(1, sim_pool.size), sim_pool.size // 2)


@pytest.fixture()
def pool_with_edge_primaries(sim_pool):
    """
    Validators are:
    [A, B, C, D]. On view == 3 primaries should be [D, A]
    After adding new node we expect, that view_change will be proposed
    and new primaries would be [A, B], because primaries will be selected
    for last stable NodeReg ([A, B, C, D])
    """
    expected_view_no = sim_pool.size - 1
    sim_pool.initiate_view_change(expected_view_no)
    sim_pool.timer.wait_for(lambda: all([n._data.view_no == expected_view_no for n in sim_pool.nodes]))
    sim_pool.timer.wait_for(lambda: all([not n._data.waiting_for_new_view for n in sim_pool.nodes]))
    # Write primaries to audit
    order_requests(sim_pool)
    return sim_pool


def test_node_txn_add_new_node(node_req_add, sim_pool, random):
    # Step 1. Prepare NODE requests and some of params to check
    # Count of NODE requests is random but less then pool size
    pool_reqs = [node_req_add(sim_pool.size + i) for i in range(random.integer(1, sim_pool.size - 1))]
    domain_reqs = create_requests(DOMAIN_REQ_COUNT)
    reqs = pool_reqs + domain_reqs
    shuffle(reqs)
    sim_pool.sim_send_requests(reqs)

    current_view_no = sim_pool.view_no
    current_pool_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=POOL_LEDGER_ID)
    current_domain_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=DOMAIN_LEDGER_ID)
    expected_view_no = current_view_no + 1
    expected_node_reg = sim_pool.validators + [Greeks[sim_pool.size + i][0] for i in range(len(pool_reqs))]

    # Step 2. Start requests ordering
    random_interval = random.integer(10, 20) * 100
    RepeatingTimer(sim_pool.timer, random_interval, partial(order_requests, sim_pool))

    # Step 3. Initiate view change process during request's ordering
    for node in sim_pool.nodes:
        sim_pool.timer.schedule(random_interval + 1000,
                                partial(node._view_changer.process_need_view_change, NeedViewChange(view_no=1)))

    # Step 4. Wait for VC completing
    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(lambda: node._view_changer._data.view_no == 1)

    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(lambda: not node._data.waiting_for_new_view)

    # Step 5. Check parameters like ordered txns count, node_reg state
    # For now we can run this checks only for old nodes with newly added.
    # Because we cannot run catchup process
    # ToDo: change this checks for making they on whole pool after INDY-2148 will be implemented

    sim_pool.timer.wait_for(lambda: all([n._data.view_no == expected_view_no for n in sim_pool.nodes]))
    sim_pool.timer.wait_for(partial(check_node_reg, sim_pool, expected_node_reg))
    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_pool_ledger_size + len(pool_reqs), POOL_LEDGER_ID))
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_domain_ledger_size + len(domain_reqs), DOMAIN_LEDGER_ID))


def test_node_txn_demote_node(node_req_demote, sim_pool, random, indexes_to_demote):
    # Step 1. Prepare NODE requests and some of params to check
    # Count of NODE requests is random but less then pool size
    demoted_names = []
    pool_reqs = []
    domain_reqs = create_requests(DOMAIN_REQ_COUNT)
    for i in indexes_to_demote:
        name, req = node_req_demote(i)
        demoted_names.append(name)
        pool_reqs.append(req)
    reqs = pool_reqs + domain_reqs
    shuffle(reqs)
    sim_pool.sim_send_requests(reqs)

    current_view_no = sim_pool.view_no
    current_pool_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=POOL_LEDGER_ID)
    current_domain_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=DOMAIN_LEDGER_ID)
    expected_view_no = current_view_no + 1
    expected_node_reg = [name for name in sim_pool._genesis_validators if name not in demoted_names]
    # Step 2. Start requests ordering
    random_interval = random.integer(10, 20) * 100
    RepeatingTimer(sim_pool.timer, random_interval, partial(order_requests, sim_pool))

    # Step 3. Initiate view change process during request's ordering
    for node in sim_pool.nodes:
        sim_pool.timer.schedule(random_interval + 1000,
                                partial(node._view_changer.process_need_view_change, NeedViewChange(view_no=1)))

    # Step 4. Wait for VC completing
    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(lambda: node._view_changer._data.view_no == 1)

    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(lambda: not node._data.waiting_for_new_view)

    # Step 5. Check parameters like ordered txns count, node_reg state
    # For now we can run this checks only for old nodes with newly added.
    # Because we cannot run catchup process
    # ToDo: change this checks for making they on whole pool after INDY-2148 will be implemented

    sim_pool.timer.wait_for(lambda: all([n._data.view_no == expected_view_no for n in sim_pool.nodes]))
    sim_pool.timer.wait_for(partial(check_node_reg, sim_pool, expected_node_reg))
    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_pool_ledger_size + len(pool_reqs), POOL_LEDGER_ID))
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_domain_ledger_size + len(domain_reqs), DOMAIN_LEDGER_ID))


@pytest.mark.skip("INDY-2148, need catchup for new added node")
def test_node_txn_edge_primaries(pool_with_edge_primaries,
                                 node_req):
    pool = pool_with_edge_primaries
    current_view_no = pool.view_no
    expected_view_no = current_view_no + 1
    expected_primaries = RoundRobinConstantNodesPrimariesSelector.select_primaries_round_robin(expected_view_no,
                                                                                               pool.validators,
                                                                                               pool.f + 1)
    req = node_req(pool.size)
    pool.sim_send_requests([req])
    random_interval = 1000
    RepeatingTimer(pool.timer, random_interval, partial(order_requests, pool))
    pool.timer.wait_for(lambda: all([n._data.view_no == expected_view_no for n in pool.nodes]))
    pool.timer.wait_for(lambda: all([n._data.primaries == expected_primaries for n in pool.nodes]))

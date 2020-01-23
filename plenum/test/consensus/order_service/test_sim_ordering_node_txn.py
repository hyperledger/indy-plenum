from functools import partial
from random import shuffle

import pytest

from plenum.common.constants import VALIDATOR, POOL_LEDGER_ID, TYPE, NODE, DATA, ALIAS, CLIENT_IP, CLIENT_PORT, NODE_IP, \
    NODE_PORT, BLS_KEY, TARGET_NYM, SERVICES, CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID
from plenum.common.messages.internal_messages import VoteForViewChange
from plenum.common.request import Request
from plenum.common.timer import RepeatingTimer
from plenum.common.txn_util import get_payload_data, get_from
from plenum.common.util import randomString
from plenum.server.consensus.primary_selector import RoundRobinConstantNodesPrimariesSelector
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.suspicion_codes import Suspicions
from plenum.test.consensus.helper import SimPool
from plenum.test.consensus.order_service.sim_helper import order_requests, \
    get_pools_ledger_size, setup_pool, check_ledger_size, create_requests
from plenum.test.greek import Greeks
from plenum.test.simulation.sim_random import DefaultSimRandom
from stp_core.common.log import getlogger

logger = getlogger()

DOMAIN_REQ_COUNT = 10


@pytest.fixture()
def sim_pool(random):
    return setup_pool(random)


# ToDo: After INDY-2148, INDY-2237 and INDY-2324 needs to increase
#  possible pool length to 3 (minimal node's count)
@pytest.fixture()
def indexes_to_demote(sim_pool, random):
    return random.sample(range(1, sim_pool.size), sim_pool.size - 4)


@pytest.fixture(
    params=[
            306974, 716913, 125696, 20762, 394118, 108575, 605859, 136899, 466216, 989378, 952818, 388333, 384805,
            125703, 646653, 428522, 928282, 893545, 108575, 605859, 136899, 726384, 466216, 811021, 413903, 851374,
            388333, 332177,
            120030,                                      # seed for demoted nodes (case for node_reg_at_beginning_of_view)
            77948, 445428,  95467, 353256, 614663,     # from mixed demotion/promotion
            752248, 659043, 550513, 141156             # general seeds (INDY-2237 and INDY-2148)
    ])
def fixed_random(request):
    return DefaultSimRandom(request.param)


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


def test_node_txn_add_new_node_random_seed(random):
    sim_pool = setup_pool(random)
    do_test_node_txn_add_new_node(sim_pool, random)


def test_node_txn_add_new_node_fixed_seed(fixed_random):
    sim_pool = setup_pool(fixed_random)
    do_test_node_txn_add_new_node(sim_pool, fixed_random)


def test_node_txn_demote_node_random_seed(random):
    sim_pool = setup_pool(random)
    do_test_node_txn_demote_node(sim_pool, random)


def test_node_txn_demote_node_fixed_seed(fixed_random):
    sim_pool = setup_pool(fixed_random)
    do_test_node_txn_demote_node(sim_pool, fixed_random)


def test_node_txn_mixed_fixed_seed(fixed_random):
    sim_pool = setup_pool(fixed_random)
    do_test_node_txn_mixed(sim_pool, fixed_random)


def test_node_txn_mixed_random_seed(random):
    sim_pool = setup_pool(random)
    do_test_node_txn_mixed(sim_pool, random)


def do_test_node_txn_add_new_node(sim_pool, random):
    # Step 1. Prepare NODE requests and some of params to check
    # Count of NODE requests is random but less then pool size
    initial_view_no = sim_pool._initial_view_no

    pool_reqs = [node_req_add(sim_pool, sim_pool.size + i) for i in range(random.integer(1, sim_pool.size - 1))]
    domain_reqs = create_requests(DOMAIN_REQ_COUNT)
    all_reqs = pool_reqs + domain_reqs
    shuffle(all_reqs)
    sim_pool.sim_send_requests(all_reqs)

    current_pool_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=POOL_LEDGER_ID)
    current_domain_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=DOMAIN_LEDGER_ID)
    expected_node_reg = sim_pool.validators + [Greeks[sim_pool.size + i][0] for i in range(len(pool_reqs))]

    # Step 2. Start ordering and VC
    do_order_and_vc(sim_pool, random, initial_view_no)

    # Step 3. Check parameters like ordered txns count, node_reg state
    # For now we can run this checks only for old nodes with newly added.
    # Because we cannot run catchup process
    # ToDo: change this checks for making they on whole pool after INDY-2148 will be implemented

    sim_pool.timer.wait_for(partial(check_node_reg, sim_pool, expected_node_reg))
    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_pool_ledger_size + len(pool_reqs), POOL_LEDGER_ID))
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_domain_ledger_size + len(domain_reqs), DOMAIN_LEDGER_ID))


def do_order_and_vc(pool, random, initial_view_no):

    # Step 1. Start requests ordering
    random_interval = random.float(1.0, 2.0)
    RepeatingTimer(pool.timer, random_interval, partial(order_requests, pool))

    # Step 2. Initiate view change process during request's ordering
    for node in pool.nodes:
        pool.timer.schedule(random_interval + 1.0,
                            partial(node._internal_bus.send, VoteForViewChange(Suspicions.DEBUG_FORCE_VIEW_CHANGE)))

    # Step 3. Wait for VC completing
    pool.timer.wait_for(lambda: all(not node._data.waiting_for_new_view
                                    and node._data.view_no > initial_view_no
                                    for node in pool.nodes),
                        max_iterations=100000)


def do_test_node_txn_demote_node(sim_pool, random):
    # Step 1. Prepare NODE requests and some of params to check
    # Count of NODE requests is random but less then pool size
    indexes_to_demote = random.sample(range(1, sim_pool.size), sim_pool.size - 4)
    random.sample(range(1, sim_pool.size), sim_pool.size - 4)
    initial_view_no = sim_pool._initial_view_no
    demoted_names = []
    pool_reqs = []
    domain_reqs = create_requests(DOMAIN_REQ_COUNT)
    for i in indexes_to_demote:
        name, req = node_req_demote(sim_pool, i)
        demoted_names.append(name)
        pool_reqs.append(req)
    all_reqs = pool_reqs + domain_reqs
    shuffle(all_reqs)
    sim_pool.sim_send_requests(all_reqs)

    current_pool_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=POOL_LEDGER_ID)
    current_domain_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=DOMAIN_LEDGER_ID)
    expected_node_reg = [name for name in sim_pool._genesis_validators if name not in demoted_names]

    # Step 2. Start ordering and VC
    do_order_and_vc(sim_pool, random, initial_view_no)

    # Step 3. Check parameters like ordered txns count, node_reg state
    # For now we can run this checks only for old nodes with newly added.
    # Because we cannot run catchup process
    # ToDo: change this checks for making they on whole pool after INDY-2148 will be implemented
    sim_pool.timer.wait_for(partial(check_node_reg, sim_pool, expected_node_reg))
    for node in sim_pool.nodes:
        if replica_name_to_node_name(node.name) in expected_node_reg:
            sim_pool.timer.wait_for(
                partial(
                    check_ledger_size, node, current_pool_ledger_size + len(pool_reqs), POOL_LEDGER_ID))
            sim_pool.timer.wait_for(
                partial(
                    check_ledger_size, node, current_domain_ledger_size + len(domain_reqs), DOMAIN_LEDGER_ID))


def do_test_node_txn_mixed(sim_pool, random):
    # Step 1. Prepare NODE requests and some of params to check
    # Count of NODE requests is random but less then pool size
    indexes_to_demote = random.sample(range(1, sim_pool.size), sim_pool.size - 4)
    initial_view_no = sim_pool._initial_view_no
    demoted_names = []
    domain_reqs = create_requests(DOMAIN_REQ_COUNT)
    pool_reqs = [node_req_add(sim_pool, sim_pool.size + i) for i in range(random.integer(1, sim_pool.f))]
    promoted_names = [Greeks[sim_pool.size + i][0] for i in range(len(pool_reqs))]

    for i in indexes_to_demote:
        name, req = node_req_demote(sim_pool, i)
        demoted_names.append(name)
        pool_reqs.append(req)
    all_reqs = domain_reqs + pool_reqs

    shuffle(all_reqs)
    sim_pool.sim_send_requests(all_reqs)

    current_pool_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=POOL_LEDGER_ID)
    current_domain_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=DOMAIN_LEDGER_ID)
    expected_node_reg = [name for name in sim_pool._genesis_validators if name not in demoted_names] + promoted_names

    # Step 2. Start ordering and VC
    do_order_and_vc(sim_pool, random, initial_view_no)

    # Step 3. Check parameters like ordered txns count, node_reg state
    # For now we can run this checks only for old nodes with newly added.
    # Because we cannot run catchup process
    # ToDo: change this checks for making they on whole pool after INDY-2148 will be implemented
    sim_pool.timer.wait_for(partial(check_node_reg, sim_pool, expected_node_reg))
    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_pool_ledger_size + len(pool_reqs), POOL_LEDGER_ID))
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_domain_ledger_size + len(domain_reqs), DOMAIN_LEDGER_ID))


@pytest.mark.skip(reason="INDY-2148 Integrate catchup service to simulation pool")
def test_demote_and_promote_back(sim_pool,
                                 random,
                                 indexes_to_demote):
    # Step 1. Prepare NODE requests and some of params to check
    # Count of NODE requests is random but less then pool size
    initial_view_no = sim_pool._initial_view_no
    demoted_names = []
    demoted_reqs = []
    promoted_reqs = []
    for i in indexes_to_demote:
        name, req = node_req_demote(sim_pool, i)
        demoted_names.append(name)
        demoted_reqs.append(req)
        _, req = node_req_promote_back(sim_pool, i)
        promoted_reqs.append(req)

    pool_reqs = []
    [pool_reqs.extend(r) for r in zip(demoted_reqs, promoted_reqs)]

    current_pool_ledger_size = get_pools_ledger_size(sim_pool, ledger_id=POOL_LEDGER_ID)
    expected_node_reg = sim_pool._genesis_validators


    do_order_and_vc(sim_pool, random, initial_view_no)

    sim_pool.timer.wait_for(partial(check_node_reg, sim_pool, expected_node_reg))
    for node in sim_pool.nodes:
        sim_pool.timer.wait_for(
            partial(
                check_ledger_size, node, current_pool_ledger_size + len(pool_reqs), POOL_LEDGER_ID))


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


def build_promote_node_back_req(node_name, txn):
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
            SERVICES: [VALIDATOR],
        },
        TARGET_NYM: target_nym
    }
    return Request(operation=operation, reqId=Request.gen_req_id(),
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


def node_req_add(sim_pool, node_index):
    return build_add_new_node_req(Greeks[sim_pool.size][0], [VALIDATOR], node_index)


def node_req_demote(sim_pool, node_index):
    node_to_demote = sim_pool.nodes[node_index]
    return node_to_demote.name.split(':')[0], \
           build_demote_node_req(Greeks[node_index][0],
                                 get_txn_data_from_ledger(
                                     node_to_demote._write_manager.database_manager.get_ledger(POOL_LEDGER_ID),
                                     Greeks[node_index][0]))


def node_req_promote_back(sim_pool, node_index):
    node_to_promote = sim_pool.nodes[node_index]
    return node_to_promote.name.split(':')[0], \
           build_promote_node_back_req(Greeks[node_index][0],
                                       get_txn_data_from_ledger(
                                       node_to_promote._write_manager.database_manager.get_ledger(POOL_LEDGER_ID),
                                       Greeks[node_index][0]))
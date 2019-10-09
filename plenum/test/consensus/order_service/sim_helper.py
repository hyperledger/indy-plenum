from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.request import ReqKey
from plenum.common.startable import Mode
from plenum.server.future_primaries_batch_handler import FuturePrimariesBatchHandler
from plenum.server.replica_helper import getNodeName
from plenum.test.consensus.helper import SimPool
from plenum.test.helper import sdk_random_request_objects
from plenum.test.testing_utils import FakeSomething
from stp_core.common.log import getlogger


logger = getlogger()
MAX_BATCH_SIZE = 2


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
        # TODO: This propagates to global config and sometimes breaks other tests
        node._orderer._config.Max3PCBatchSize = MAX_BATCH_SIZE
        node._orderer._config.CHK_FREQ = 5
        node._orderer._config.LOG_SIZE = 3 * node._orderer._config.CHK_FREQ
        setup_consensus_data(node._data)
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
                assert node._write_manager.database_manager.get_ledger(ledger_id).uncommitted_root_hash == \
                    another_node._write_manager.database_manager.get_ledger(ledger_id).uncommitted_root_hash


def check_batch_count(node, expected_pp_seq_no):
    logger.debug("Node: {}, Actual pp_seq_no: {}, Expected pp_seq_no is: {}".format(node,
                                                                                    node._orderer.last_ordered_3pc[1],
                                                                                    expected_pp_seq_no))
    return node._orderer.last_ordered_3pc[1] == expected_pp_seq_no


def order_requests(pool):
    primary_nodes = [n for n in pool.nodes if n._data.is_primary]
    if primary_nodes and not primary_nodes[0]._data.waiting_for_new_view:
        primary_nodes[0]._orderer.send_3pc_batch()

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID
from plenum.common.request import ReqKey
from plenum.common.startable import Mode
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.test.consensus.helper import SimPool
from plenum.test.helper import sdk_random_request_objects
from stp_core.common.log import getlogger


logger = getlogger()
MAX_BATCH_SIZE = 2
CHK_FREQ = 20


def create_requests(count):
    return sdk_random_request_objects(count, CURRENT_PROTOCOL_VERSION)


def create_pool(random):
    pool_size = random.integer(4, 8)
    pool = SimPool(pool_size, random)
    return pool


def setup_consensus_data(cd):
    cd.node_mode = Mode.participating


def update_config(config, updated_args: dict):
    for name, value in updated_args.items():
        setattr(config, name, value)


def setup_pool(random, config_args=None):
    pool = create_pool(random)
    general_config  = {'Max3PCBatchSize': MAX_BATCH_SIZE,
                       'CHK_FREQ': CHK_FREQ,
                       'LOG_SIZE': 3 * CHK_FREQ}
    if config_args:
        general_config.update(config_args)

    for node in pool.nodes:
        # TODO: This propagates to global config and sometimes breaks other tests
        update_config(node._orderer._config, general_config)
        setup_consensus_data(node._data)
        node._orderer._network._connecteds = list(set(node._data.validators) -
                                                  {replica_name_to_node_name(node._data.name)})

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


def check_ledger_size(node, expected_ledger_size, ledger_id=DOMAIN_LEDGER_ID):
    current_ledger_size = node._write_manager.database_manager.get_ledger(ledger_id).size
    logger.debug("Node: {}, Actual ledger_size: {}, Expected ledger_size is: {}".format(node,
                                                                                    current_ledger_size,
                                                                                    expected_ledger_size))
    return current_ledger_size == expected_ledger_size


def get_pools_ledger_size(pool, ledger_id=DOMAIN_LEDGER_ID):
    sizes = set([node._write_manager.database_manager.get_ledger(ledger_id).size for node in pool.nodes])
    assert len(sizes) == 1
    return sizes.pop()


def order_requests(pool):
    primary_nodes = [n for n in pool.nodes if n._data.is_primary]
    if primary_nodes and not primary_nodes[0]._data.waiting_for_new_view:
        primary_nodes[0]._orderer.send_3pc_batch()
    for n in pool.nodes:
        n._orderer.dequeue_pre_prepares()

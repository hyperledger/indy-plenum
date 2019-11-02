import pytest

from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.test.consensus.helper import create_test_write_req_manager
from plenum.test.greek import genNodeNames
from plenum.test.helper import create_pool_txn_data


@pytest.fixture()
def initial_nodes():
    return genNodeNames(4)


@pytest.fixture()
def write_req_manager(initial_nodes):
    genesis_txns = create_pool_txn_data(
        node_names=initial_nodes,
        crypto_factory=create_default_bls_crypto_factory(),
        get_free_port=lambda: 8090)['txns']
    return create_test_write_req_manager(initial_nodes[0], genesis_txns)


@pytest.fixture()
def node_reg_handler(write_req_manager):
    return write_req_manager.node_reg_handler

@pytest.fixture()
def init_node_reg_handler(write_req_manager):
    write_req_manager.on_catchup_finished()
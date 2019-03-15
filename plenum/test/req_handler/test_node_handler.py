import pytest

from plenum.common.constants import NODE, BLS_KEY, BLS_KEY_PROOF, TARGET_NYM, NODE_IP, NODE_PORT, CLIENT_IP, \
    CLIENT_PORT, ALIAS
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.util import randomString
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.node_handler import NodeHandler
from plenum.test.testing_utils import FakeSomething
from state.state import State


@pytest.fixture(scope='module')
def node_handler():
    data_manager = DatabaseManager()
    bls = FakeSomething()
    handler = NodeHandler(data_manager, bls)
    state = State()
    state.txn_list = {}
    state.get = lambda key, is_committed: state.txn_list.get(key, None)
    state.set = lambda key, value: state.txn_list.update({key: value})
    data_manager.register_new_database(handler.ledger_id,
                                       FakeSomething(),
                                       state)
    return handler


@pytest.fixture(scope='function')
def node_request():
    return Request(identifier=randomString(),
                   reqId=5,
                   operation={'type': NODE,
                              'dest': randomString(),
                              TARGET_NYM: randomString(),
                              'data': {BLS_KEY: randomString(),
                                       BLS_KEY_PROOF: randomString(),
                                       ALIAS: 'smth',
                                       NODE_IP: 1,
                                       NODE_PORT: 2,
                                       CLIENT_IP: 3,
                                       CLIENT_PORT: 4
                                       }})


def test_node_handler_static_validation_fails(node_handler, node_request):
    del node_request.operation['data'][BLS_KEY]
    node_request.operation['data'][BLS_KEY_PROOF] = randomString()
    with pytest.raises(InvalidClientRequest):
        node_handler.static_validation(node_request)

    del node_request.operation['data'][BLS_KEY_PROOF]
    node_request.operation['data'][BLS_KEY] = randomString()
    with pytest.raises(InvalidClientRequest):
        node_handler.static_validation(node_request)

    node_request.operation['data'][BLS_KEY_PROOF] = randomString()
    node_request.operation['data'][BLS_KEY] = randomString()
    node_handler._verify_bls_key_proof_of_possession = lambda blskey_proof, blskey: False

    with pytest.raises(InvalidClientRequest):
        node_handler.static_validation(node_request)


def test_node_handler_static_validation_passes(node_handler, node_request):
    del node_request.operation['data'][BLS_KEY]
    del node_request.operation['data'][BLS_KEY_PROOF]
    node_handler.static_validation(node_request)

    node_request.operation['data'][BLS_KEY_PROOF] = randomString()
    node_request.operation['data'][BLS_KEY] = randomString()
    node_handler._verify_bls_key_proof_of_possession = lambda blskey_proof, blskey: True
    node_handler.static_validation(node_request)


def test_node_handler_dynamic_validation_new_node_fails_missing(node_handler,
                                                                node_request):
    del node_request.operation['data'][CLIENT_PORT]
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request)
    e.match('Missing some of')


def test_node_handler_dynamic_validation_new_node_fails_same_ha(node_handler,
                                                                node_request):
    node_request.operation['data'][CLIENT_IP] = 1
    node_request.operation['data'][CLIENT_PORT] = 2
    node_request.operation['data'][NODE_IP] = 1
    node_request.operation['data'][NODE_PORT] = 2
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request)
    e.match('node and client ha cannot be same')


def test_node_handler_dynamic_validation_new_node_fails_has_node(node_handler,
                                                                 node_request):
    node_handler._is_steward = lambda nym, is_committed: True
    node_handler._steward_has_node = lambda origin: True

    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request)
    e.match('already has a node')


def test_node_handler_dynamic_validation_new_node_fails_conflict(node_handler,
                                                                 node_request):
    node_handler._steward_has_node = lambda origin: False
    node_handler._is_node_data_conflicting = lambda data: 'smth'

    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request)
    e.match('existing data has conflicts with')


def test_node_handler_dynamic_validation_new_node_passes(node_handler,
                                                         node_request):
    node_handler._is_node_data_conflicting = lambda data, updating_nym=None: None
    node_handler.dynamic_validation(node_request)


def test_node_handler_dynamic_validation_update_node_fails_same_data(node_handler,
                                                                     node_request):
    node_handler._get_node_data = lambda *args, **kwargs: True
    node_handler._is_steward_of_node = lambda steward_nym, node_nym, is_committed: True
    node_handler._is_node_data_same = lambda node_nym, new_data, is_committed: True
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request)
    e.match('node already has the same data as requested')


def test_node_handler_dynamic_validation_update_node_fails_conflict_data(node_handler,
                                                                         node_request):
    node_handler._get_node_data = lambda *args, **kwargs: True
    node_handler._is_node_data_same = lambda node_nym, new_data, is_committed: False
    node_handler._is_node_data_conflicting = lambda new_data, updating_nym=None: True
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request)
    e.match('existing data has conflicts with')


def test_node_handler_dynamic_validation_update_node_passes(node_handler,
                                                            node_request):
    node_handler._is_node_data_conflicting = lambda new_data, updating_nym=None: False
    node_handler.dynamic_validation(node_request)

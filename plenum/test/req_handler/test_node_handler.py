import pytest

from plenum.common.constants import NODE, BLS_KEY, BLS_KEY_PROOF, TARGET_NYM, NODE_IP, NODE_PORT, CLIENT_IP, \
    CLIENT_PORT, ALIAS, DATA, IDENTIFIER, SERVICES, VALIDATOR
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, append_txn_metadata
from plenum.common.util import randomString
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.node_handler import NodeHandler
from plenum.test.testing_utils import FakeSomething
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture(scope='module')
def node_handler():
    data_manager = DatabaseManager()
    bls = FakeSomething()
    handler = NodeHandler(data_manager, bls)
    state = PruningState(KeyValueStorageInMemory())
    data_manager.register_new_database(handler.ledger_id,
                                       FakeSomething(),
                                       state)
    return handler


@pytest.fixture(scope='function')
def node_request():
    return Request(identifier='12121212121212',
                   reqId=5,
                   operation={'type': NODE,
                              'dest': '12121212121212',
                              TARGET_NYM: randomString(),
                              'data': {BLS_KEY: randomString(),
                                       BLS_KEY_PROOF: randomString(),
                                       ALIAS: 'smth',
                                       NODE_IP: 1,
                                       NODE_PORT: 2,
                                       CLIENT_IP: 3,
                                       CLIENT_PORT: 4
                                       }})


def test_update_state(node_handler, node_request):
    seq_no = 1
    txn_time = 1560241033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    expected_data = node_request.operation[DATA]
    expected_data.update({IDENTIFIER: node_request.identifier})

    node_handler.update_state(txn, None, node_request)
    tmp = node_handler.get_from_state(
        node_handler.gen_state_key(txn))
    assert set(tmp.items()) == set(expected_data.items())


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


def test_node_handler_static_validation_fail_dest_and_key(node_handler, node_request):
    del node_request.operation['data'][BLS_KEY]
    del node_request.operation['data'][BLS_KEY_PROOF]
    node_request.operation[TARGET_NYM] = '0' * 15
    with pytest.raises(InvalidClientRequest, match="Node's dest is not correct Ed25519 key"):
        node_handler.static_validation(node_request)

    node_request.operation['data'][BLS_KEY_PROOF] = randomString()
    node_request.operation['data'][BLS_KEY] = randomString()
    node_handler._verify_bls_key_proof_of_possession = lambda blskey_proof, blskey: True

    node_request.operation[TARGET_NYM] = '0000000000'
    with pytest.raises(InvalidClientRequest, match="Node's dest is not correct Ed25519 key"):
        node_handler.static_validation(node_request)


def test_node_handler_dynamic_validation_new_node_fails_missing(node_handler,
                                                                node_request):
    del node_request.operation['data'][CLIENT_PORT]
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request, 0)
    e.match('Missing some of')


def test_node_handler_dynamic_validation_new_node_fails_same_ha(node_handler,
                                                                node_request):
    node_request.operation['data'][CLIENT_IP] = 1
    node_request.operation['data'][CLIENT_PORT] = 2
    node_request.operation['data'][NODE_IP] = 1
    node_request.operation['data'][NODE_PORT] = 2
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request, 0)
    e.match('node and client ha cannot be same')


def test_node_handler_dynamic_validation_new_node_succeeds_has_node_not_validator(node_handler):
    # Create Request
    node_request = Request(identifier='12121212121212',
                           reqId=5,
                           operation={
                               'type': NODE,
                               TARGET_NYM: '2m3hkiDTTseSLxw6arffwyMrfTpoaadEhjLxGFSz4RLa',
                               'data': {
                                   BLS_KEY: randomString(),
                                   BLS_KEY_PROOF: randomString(),
                                   ALIAS: 'Node1',
                                   NODE_IP: 2,
                                   NODE_PORT: 3,
                                   CLIENT_IP: 4,
                                   CLIENT_PORT: 5,
                                   SERVICES: [VALIDATOR]
                               }})

    # Modify bls verifier
    node_handler.bls_crypto_verifier = None

    # write node transaction to ledger
    seq_no = 1
    txn_time = 1560241033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)

    # demote node
    seq_no = 2
    txn_time = 1560242033
    txn_id = "id"
    node_request.reqId = 6
    node_request.operation['data'][SERVICES] = []
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)

    # Create New Request
    node_request.reqId = 7
    node_request.operation[TARGET_NYM] = '2EcmfdGn5zE2drojZxL9EVaWU2JeKMpYMRfsxLwD2jvG'
    node_request.operation['data'][BLS_KEY] = randomString()
    node_request.operation['data'][BLS_KEY_PROOF] = randomString()
    node_request.operation['data'][ALIAS] = 'Node2'
    node_request.operation['data'][NODE_IP] = 5
    node_request.operation['data'][CLIENT_IP] = 6
    node_request.operation['data'][SERVICES] = [VALIDATOR]

    # write new node transaction to ledger
    seq_no = 3
    txn_time = 1560243033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)
    # celebrate success


def test_node_handler_dynamic_validation_new_node_fails_has_node(node_handler):
    # Create Request
    node_request = Request(identifier='23232323232323',
                           reqId=6,
                           operation={
                               'type': NODE,
                               TARGET_NYM: '2m3hkiDTTseSLxw6arffwyMrfTpoaadEhjLxGFSz4RLc',
                               'data': {
                                   BLS_KEY: randomString(),
                                   BLS_KEY_PROOF: randomString(),
                                   ALIAS: 'Node3',
                                   NODE_IP: 7,
                                   NODE_PORT: 8,
                                   CLIENT_IP: 9,
                                   CLIENT_PORT: 10,
                                   SERVICES: [VALIDATOR]
                               }})

    # Modify bls verifier
    node_handler.bls_crypto_verifier = None

    # write node transaction to ledger
    seq_no = 4
    txn_time = 1560244033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)

    # Create New Request
    node_request.reqId = 7
    node_request.operation[TARGET_NYM] = '2EcmfdGn5zE2drojZxL9EVaWU2JeKMpYMRfsxLwD2jvt'
    node_request.operation['data'][BLS_KEY] = randomString()
    node_request.operation['data'][BLS_KEY_PROOF] = randomString()
    node_request.operation['data'][ALIAS] = 'Node4'
    node_request.operation['data'][NODE_IP] = 11
    node_request.operation['data'][CLIENT_IP] = 12

    # write new node transaction to ledger
    seq_no = 5
    txn_time = 1560245033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request, 0)
    e.match("23232323232323 already has node b'2m3hkiDTTseSLxw6arffwyMrfTpoaadEhjLxGFSz4RLc' as a validator")


def test_node_handler_dynamic_validation_update_node_fails_has_node(node_handler):
    # Create Request
    node_request = Request(identifier='34343434343434',
                           reqId=8,
                           operation={
                               'type': NODE,
                               TARGET_NYM: '2EcmfdGn5zE2drojZxL9EVaWU2JeKMpYMRfsxLwD2jve',
                               'data': {
                                   BLS_KEY: randomString(),
                                   BLS_KEY_PROOF: randomString(),
                                   ALIAS: 'Node5',
                                   NODE_IP: 13,
                                   NODE_PORT: 14,
                                   CLIENT_IP: 15,
                                   CLIENT_PORT: 16,
                                   SERVICES: [VALIDATOR]
                               }})

    # Modify bls verifier
    node_handler.bls_crypto_verifier = None

    # write node transaction to ledger
    seq_no = 6
    txn_time = 1560246033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)

    # demote node
    seq_no = 7
    txn_time = 1560247033
    txn_id = "id"
    node_request.reqId = 9
    node_request.operation['data'][SERVICES] = []
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)

    # Create New Request
    node_request.reqId = 10
    node_request.operation[TARGET_NYM] = '2EcmfdGn5zE2drojZxL9EVaWU2JeKMpYMRfsxLwD2jvb'
    node_request.operation['data'][BLS_KEY] = randomString()
    node_request.operation['data'][BLS_KEY_PROOF] = randomString()
    node_request.operation['data'][ALIAS] = 'Node6'
    node_request.operation['data'][NODE_IP] = 16
    node_request.operation['data'][CLIENT_IP] = 17
    node_request.operation['data'][SERVICES] = [VALIDATOR]

    # write new node transaction to ledger
    seq_no = 8
    txn_time = 1560248033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)

    # try to promote original node
    node_request = Request(identifier='34343434343434',
                           reqId=11,
                           operation={
                               'type': NODE,
                               TARGET_NYM: '2EcmfdGn5zE2drojZxL9EVaWU2JeKMpYMRfsxLwD2jve',
                               'data': {
                                   BLS_KEY: randomString(),
                                   BLS_KEY_PROOF: randomString(),
                                   ALIAS: 'Node5',
                                   NODE_IP: 13,
                                   NODE_PORT: 14,
                                   CLIENT_IP: 15,
                                   CLIENT_PORT: 16,
                                   SERVICES: [VALIDATOR]
                               }})
    seq_no = 9
    txn_time = 1560249033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request, 0)
    e.match("34343434343434 already has node b'2EcmfdGn5zE2drojZxL9EVaWU2JeKMpYMRfsxLwD2jvb' as a validator")


def test_node_handler_dynamic_validation_update_node_succeeds_old_node(node_handler):
    # Create Request
    node_request = Request(identifier='45454545454545',
                           reqId=8,
                           operation={
                               'type': NODE,
                               TARGET_NYM: '2EcmfdGn5zE2drojZxL9EVaWU2JeKMpYMRfsxLwD2jve',
                               'data': {
                                   BLS_KEY: randomString(),
                                   BLS_KEY_PROOF: randomString(),
                                   ALIAS: 'Node5',
                                   NODE_IP: 13,
                                   NODE_PORT: 14,
                                   CLIENT_IP: 15,
                                   CLIENT_PORT: 16,
                                   SERVICES: [VALIDATOR]
                               }})

    # Modify bls verifier
    node_handler.bls_crypto_verifier = None

    # write node transaction to ledger
    seq_no = 6
    txn_time = 1560246033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)

    # demote node
    seq_no = 7
    txn_time = 1560247033
    txn_id = "id"
    node_request.reqId = 9
    node_request.operation['data'][SERVICES] = []
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    node_handler.update_state(txn, None, node_request)

    # promote original node
    node_request = Request(identifier='45454545454545',
                           reqId=11,
                           operation={
                               'type': NODE,
                               TARGET_NYM: '2EcmfdGn5zE2drojZxL9EVaWU2JeKMpYMRfsxLwD2jve',
                               'data': {
                                   BLS_KEY: randomString(),
                                   BLS_KEY_PROOF: randomString(),
                                   ALIAS: 'Node5',
                                   NODE_IP: 13,
                                   NODE_PORT: 14,
                                   CLIENT_IP: 15,
                                   CLIENT_PORT: 16,
                                   SERVICES: [VALIDATOR]
                               }})
    seq_no = 9
    txn_time = 1560249033
    txn_id = "id"
    txn = reqToTxn(node_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    node_handler.static_validation(node_request)
    node_handler.dynamic_validation(node_request, 0)
    # celebrate success


def test_node_handler_dynamic_validation_new_node_fails_conflict(node_handler,
                                                                 node_request):
    node_handler._steward_has_validator_node = lambda origin: False
    node_handler._is_node_data_conflicting = lambda data: 'smth'

    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request, 0)
    e.match('existing data has conflicts with')


def test_node_handler_dynamic_validation_new_node_passes(node_handler,
                                                         node_request):
    node_handler._is_node_data_conflicting = lambda data, updating_nym=None: None
    node_handler.dynamic_validation(node_request, 0)


def test_node_handler_dynamic_validation_update_node_fails_same_data(node_handler,
                                                                     node_request):
    node_handler.get_from_state= lambda *args, **kwargs: True
    node_handler._is_steward_of_node = lambda steward_nym, node_nym, is_committed: True
    node_handler._is_node_data_same = lambda node_nym, new_data, is_committed: True
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request, 0)
    e.match('node already has the same data as requested')


def test_node_handler_dynamic_validation_update_node_fails_conflict_data(node_handler,
                                                                         node_request):
    node_handler.get_from_state = lambda *args, **kwargs: True
    node_handler._is_node_data_same = lambda node_nym, new_data, is_committed: False
    node_handler._is_node_data_conflicting = lambda new_data, updating_nym=None: True
    with pytest.raises(UnauthorizedClientRequest) as e:
        node_handler.dynamic_validation(node_request, 0)
    e.match('existing data has conflicts with')

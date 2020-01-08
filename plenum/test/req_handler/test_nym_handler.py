import pytest as pytest

from plenum.common.constants import ROLE, STEWARD, NYM, TXN_TYPE
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_reply_nym
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.nym_handler import NymHandler
from plenum.server.request_handlers.utils import get_nym_details, get_role, is_steward
from plenum.test.req_handler.helper import create_nym_txn, update_nym
from plenum.test.testing_utils import FakeSomething
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture(scope="function")
def nym_handler(tconf):
    data_manager = DatabaseManager()
    handler = NymHandler(tconf, data_manager)
    state = PruningState(KeyValueStorageInMemory())
    data_manager.register_new_database(handler.ledger_id,
                                       FakeSomething(),
                                       state)
    return handler


def test_dynamic_validation(nym_handler):
    identifier = "test_identifier"
    update_nym(nym_handler.state, identifier, STEWARD)
    request = Request(identifier=identifier,
                      operation={TXN_TYPE: NYM,
                                 ROLE: ""})
    nym_handler.dynamic_validation(request, 0)


def test_dynamic_validation_msg_from_not_steward(nym_handler):
    identifier = "test_identifier"
    update_nym(nym_handler.state, identifier, "")
    request = Request(identifier=identifier,
                      operation={
                          TXN_TYPE: NYM, ROLE: ""})

    with pytest.raises(UnauthorizedClientRequest) as e:
        nym_handler.dynamic_validation(request, 0)
    assert "Only Steward is allowed to do these transactions" \
           in e._excinfo[1].args[0]


def test_dynamic_validation_steward_create_steward_before_limit(nym_handler):
    identifier = "test_identifier"
    update_nym(nym_handler.state, identifier, STEWARD)
    request = Request(identifier=identifier,
                      operation={TXN_TYPE: NYM,
                                 ROLE: STEWARD})
    nym_handler.dynamic_validation(request, 0)


def test_dynamic_validation_steward_create_steward_after_limit(nym_handler):
    identifier = "test_identifier"
    update_nym(nym_handler.state, identifier, STEWARD)
    old_steward_threshold = nym_handler.config.stewardThreshold
    nym_handler.config.stewardThreshold = 1
    nym_handler._steward_count = 1

    request = Request(identifier=identifier,
                      operation={TXN_TYPE: NYM,
                                 ROLE: STEWARD})

    with pytest.raises(UnauthorizedClientRequest) as e:
        nym_handler.dynamic_validation(request, 0)
    assert "New stewards cannot be added by other stewards as there are already" \
           in e._excinfo[1].args[0]

    nym_handler.config.stewardThreshold = old_steward_threshold


def test_update_state(nym_handler):
    txns = []
    for i in range(5):
        update_nym(nym_handler.state, "identifier{}".format(i), STEWARD)

    for txn in txns:
        nym_data = get_nym_details(nym_handler.state, get_reply_nym(txn))
        assert nym_data[ROLE] == STEWARD


def test_update_nym(nym_handler):
    identifier = "identifier"
    txn1 = create_nym_txn(identifier, STEWARD)
    txn2 = create_nym_txn(identifier, "")

    update_nym(nym_handler.state, identifier, STEWARD)
    nym_data = get_nym_details(nym_handler.state, identifier)
    assert get_payload_data(txn1)[ROLE] == nym_data[ROLE]

    update_nym(nym_handler.state, identifier, "")
    nym_data = get_nym_details(nym_handler.state, identifier)
    assert get_payload_data(txn2)[ROLE] == nym_data[ROLE]


def test_get_role(nym_handler):
    identifier = "test_identifier"
    target_nym = "test_target_nym"
    txn = create_nym_txn(identifier, STEWARD, target_nym)
    nym_handler.update_state(txn, None, None)
    nym_data = get_role(nym_handler.state, target_nym)
    assert nym_data == STEWARD


def test_get_role_nym_without_role(nym_handler):
    identifier = "test_identifier"
    target_nym = "test_target_nym"
    txn = create_nym_txn(identifier, "", target_nym)
    nym_handler.update_state(txn, None, None)
    nym_data = get_role(nym_handler.state, target_nym)
    assert not nym_data


def test_get_role_without_nym_data(nym_handler):
    identifier = "test_identifier"
    nym_data = get_role(nym_handler.state, identifier)
    assert not nym_data


def test_is_steward(nym_handler):
    identifier = "test_identifier"
    target_nym = "test_target_nym"
    txn = create_nym_txn(identifier, STEWARD, target_nym)
    nym_handler.update_state(txn, None, None)
    assert is_steward(nym_handler.state, target_nym)
    assert not is_steward(nym_handler.state, "other_identifier")

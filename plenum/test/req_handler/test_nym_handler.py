import pytest as pytest

from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, reqToTxn, get_reply_nym
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.nym_handler import NymHandler
from plenum.test.testing_utils import FakeSomething
from state.state import State


@pytest.fixture(scope="function")
def nym_handler(tconf):
    data_manager = DatabaseManager()
    handler = NymHandler(tconf, data_manager)
    state = State()
    state.txn_list = {}
    state.get = lambda key, isCommitted: state.txn_list.get(key, None)
    state.set = lambda key, value: state.txn_list.update({key: value})
    data_manager.register_new_database(handler.ledger_id,
                                       FakeSomething(),
                                       state)
    return handler


def test_dynamic_validation(nym_handler):
    identifier = "test_identifier"
    nym_handler.updateNym(identifier, _create_nym_txn(identifier, STEWARD))
    request = Request(identifier=identifier,
                      operation={ROLE: ""})
    nym_handler.dynamic_validation(request)


def test_dynamic_validation_msg_from_not_steward(nym_handler):
    identifier = "test_identifier"
    nym_handler.updateNym(identifier, _create_nym_txn(identifier, ""))
    request = Request(identifier=identifier,
                      operation={ROLE: ""})

    with pytest.raises(UnauthorizedClientRequest) as e:
        nym_handler.dynamic_validation(request)
    assert "Only Steward is allowed to do these transactions" \
           in e._excinfo[1].args[0]


def test_dynamic_validation_steward_create_steward_before_limit(nym_handler):
    identifier = "test_identifier"
    nym_handler.updateNym(identifier, _create_nym_txn(identifier, STEWARD))
    request = Request(identifier=identifier,
                      operation={ROLE: STEWARD})
    nym_handler.dynamic_validation(request)


def test_dynamic_validation_steward_create_steward_after_limit(nym_handler):
    identifier = "test_identifier"
    nym_handler.updateNym(identifier, _create_nym_txn(identifier, STEWARD))
    old_steward_threshold = nym_handler.config.stewardThreshold
    nym_handler.config.stewardThreshold = 1

    request = Request(identifier=identifier,
                      operation={ROLE: STEWARD})

    with pytest.raises(UnauthorizedClientRequest) as e:
        nym_handler.dynamic_validation(request)
    assert "New stewards cannot be added by other stewards as there are already" \
           in e._excinfo[1].args[0]

    nym_handler.config.stewardThreshold = old_steward_threshold


def test_update_state(nym_handler):
    txns = []
    for i in range(5):
        txns.append(_create_nym_txn("identifier{}".format(i), STEWARD, str(i)))
    nym_handler.updateState(txns)

    for txn in txns:
        nym_data = nym_handler.getNymDetails(nym_handler.state, get_reply_nym(txn))
        assert nym_data[ROLE] == STEWARD


def test_update_nym(nym_handler):
    identifier = "identifier"
    txn1 = _create_nym_txn(identifier, STEWARD)
    txn2 = _create_nym_txn(identifier, "")

    nym_handler.updateNym(identifier, txn1)
    nym_data = nym_handler.getNymDetails(nym_handler.state, identifier)
    assert get_payload_data(txn1)[ROLE] == nym_data[ROLE]

    nym_handler.updateNym(identifier, txn2)
    nym_data = nym_handler.getNymDetails(nym_handler.state, identifier)
    assert get_payload_data(txn2)[ROLE] == nym_data[ROLE]


def test_get_role(nym_handler):
    identifier = "test_identifier"
    nym_handler.updateNym(identifier, _create_nym_txn(identifier, STEWARD))
    nym_data = nym_handler.get_role(nym_handler.state, identifier, STEWARD)
    assert nym_data[ROLE] == STEWARD


def test_get_role_nym_without_role(nym_handler):
    identifier = "test_identifier"
    nym_handler.updateNym(identifier, _create_nym_txn(identifier, ""))
    nym_data = nym_handler.get_role(nym_handler.state, identifier, STEWARD)
    assert not nym_data


def test_get_role_without_nym_data(nym_handler):
    identifier = "test_identifier"
    nym_data = nym_handler.get_role(nym_handler.state, identifier, STEWARD)
    assert not nym_data


def test_is_steward(nym_handler):
    identifier = "test_identifier"
    nym_handler.updateNym(identifier, _create_nym_txn(identifier, STEWARD))
    assert nym_handler.isSteward(nym_handler.state, identifier)
    assert not nym_handler.isSteward(nym_handler.state, "other_identifier")


def _create_nym_txn(identifier, role, nym="TARGET_NYM"):
    return reqToTxn(Request(identifier=identifier,
                            operation={ROLE: role,
                                       TXN_TYPE: NYM,
                                       TARGET_NYM: nym}))

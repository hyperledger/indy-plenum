import pytest as pytest

from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE, TXN_AUTHOR_AGREEMENT, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, TRUSTEE, DOMAIN_LEDGER_ID
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.txn_author_agreement_handler import TxnAuthorAgreementHandler
from plenum.server.request_handlers.utils import nym_to_state_key
from plenum.test.testing_utils import FakeSomething
from state.state import State


@pytest.fixture(scope="function")
def domain_state(tconf):
    state = State()
    state.txn_list = {}
    state.get = lambda key, isCommitted=False: state.txn_list.get(key, None)
    state.set = lambda key, value, isCommitted=False: state.txn_list.update({key: value})
    return state


@pytest.fixture(scope="function")
def txn_author_agreement_handler(tconf, domain_state):
    data_manager = DatabaseManager()
    handler = TxnAuthorAgreementHandler(data_manager, FakeSomething())
    state = State()
    state.txn_list = {}
    state.get = lambda key, isCommitted=False: state.txn_list.get(key, None)
    state.set = lambda key, value, isCommitted=False: state.txn_list.update({key: value})
    data_manager.register_new_database(handler.ledger_id,
                                       FakeSomething(),
                                       state)
    data_manager.register_new_database(DOMAIN_LEDGER_ID,
                                       FakeSomething(),
                                       domain_state)
    return handler


@pytest.fixture(scope="function")
def set_aml(txn_author_agreement_handler):
    txn_author_agreement_handler.state.set(StaticTAAHelper.state_path_taa_aml_latest(), "value")


@pytest.fixture(scope="function")
def taa_request(tconf, txn_author_agreement_handler, domain_state):
    identifier = "identifier"
    update_nym(domain_state, identifier, TRUSTEE)
    return Request(identifier=identifier,
                   operation={TXN_TYPE: TXN_AUTHOR_AGREEMENT,
                              TXN_AUTHOR_AGREEMENT_TEXT: "text",
                              TXN_AUTHOR_AGREEMENT_VERSION: "version"})


def test_static_validation(txn_author_agreement_handler, taa_request):
    txn_author_agreement_handler.static_validation(taa_request)


def test_dynamic_validation(txn_author_agreement_handler, taa_request, set_aml):
    txn_author_agreement_handler.dynamic_validation(taa_request)


def test_dynamic_validation_without_aml(txn_author_agreement_handler, taa_request):
    with pytest.raises(InvalidClientRequest,
                       match="TAA txn is forbidden until TAA AML is set. Send TAA AML first."):
        txn_author_agreement_handler.dynamic_validation(taa_request)


def test_dynamic_validation_from_steward(txn_author_agreement_handler, domain_state,
                                         taa_request, set_aml):
    identifier = "test_identifier"
    update_nym(domain_state, identifier, STEWARD)
    taa_request._identifier = identifier
    with pytest.raises(UnauthorizedClientRequest,
                       match="Only trustee can update transaction author agreement and AML"):
        txn_author_agreement_handler.dynamic_validation(taa_request)


def test_dynamic_validation_with_not_unique_version(txn_author_agreement_handler, taa_request, set_aml):
    version = taa_request.operation[TXN_AUTHOR_AGREEMENT_VERSION]
    txn_author_agreement_handler.state.set(StaticTAAHelper.state_path_taa_version(version), "{}".encode())
    with pytest.raises(InvalidClientRequest,
                       match="Changing existing version of transaction author agreement is forbidden"):
        txn_author_agreement_handler.dynamic_validation(taa_request)


def _create_nym_txn(identifier, role, nym="TARGET_NYM"):
    return reqToTxn(Request(identifier=identifier,
                            operation={ROLE: role,
                                       TXN_TYPE: NYM,
                                       TARGET_NYM: nym}))


def update_nym(state, identifier, role):
    state.set(nym_to_state_key(identifier),
              domain_state_serializer.serialize(
                  _create_nym_txn(identifier, role)['txn']['data']))

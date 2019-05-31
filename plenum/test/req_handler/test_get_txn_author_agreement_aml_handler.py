import pytest as pytest

from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE, TXN_AUTHOR_AGREEMENT, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, TRUSTEE, DOMAIN_LEDGER_ID, TXN_AUTHOR_AGREEMENT_AML, \
    AML_VERSION, AML, AML_CONTEXT
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, reqToTxn, get_reply_nym
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.get_txn_author_agreement_aml_handler import GetTxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.txn_author_agreement_aml_handler import TxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.get_txn_author_agreement_aml_handler import TxnAuthorAgreementHandler
from plenum.server.request_handlers.utils import get_nym_details, get_role, is_steward, nym_to_state_key
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
def get_txn_author_agreement_aml_handler(tconf, domain_state):
    data_manager = DatabaseManager()
    handler = GetTxnAuthorAgreementAmlHandler(data_manager, FakeSomething())
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
def aml_request(tconf, get_txn_author_agreement_aml_handler, domain_state):
    identifier = "identifier"
    update_nym(domain_state, identifier, TRUSTEE)
    return Request(identifier=identifier,
                   operation={TXN_TYPE: TXN_AUTHOR_AGREEMENT_AML,
                              AML_VERSION: "AML_VERSION",
                              AML: {"test": "test"},
                              AML_CONTEXT: "AML_CONTEXT"})


def test_static_validation(get_txn_author_agreement_aml_handler, aml_request):
    get_txn_author_agreement_aml_handler.static_validation(aml_request)


def test_static_validation_with_empty_aml(get_txn_author_agreement_aml_handler, aml_request):
    aml_request.operation[AML] = {}
    with pytest.raises(InvalidClientRequest,
                       match="TXN_AUTHOR_AGREEMENT_AML request must contain at least one acceptance mechanism"):
        get_txn_author_agreement_aml_handler.static_validation(aml_request)


def test_dynamic_validation(get_txn_author_agreement_aml_handler, aml_request):
    get_txn_author_agreement_aml_handler.dynamic_validation(aml_request)


def test_dynamic_validation_with_not_unique_aml(get_txn_author_agreement_aml_handler, aml_request):
    version = aml_request.operation[AML_VERSION]
    get_txn_author_agreement_aml_handler.state.set(StaticTAAHelper.state_path_taa_aml_version(version), "{}")
    with pytest.raises(InvalidClientRequest,
                       match="Version of TAA AML must be unique and it cannot be modified"):
        get_txn_author_agreement_aml_handler.dynamic_validation(aml_request)


def test_dynamic_validation_from_steward(get_txn_author_agreement_aml_handler, domain_state, aml_request):
    identifier = "test_identifier"
    update_nym(domain_state, identifier, STEWARD)
    aml_request._identifier = identifier
    with pytest.raises(UnauthorizedClientRequest,
                       match="Only trustee can update transaction author agreement and AML"):
        get_txn_author_agreement_aml_handler.dynamic_validation(aml_request)


def _create_nym_txn(identifier, role, nym="TARGET_NYM"):
    return reqToTxn(Request(identifier=identifier,
                            operation={ROLE: role,
                                       TXN_TYPE: NYM,
                                       TARGET_NYM: nym}))


def update_nym(state, identifier, role):
    state.set(nym_to_state_key(identifier),
              domain_state_serializer.serialize(
                  _create_nym_txn(identifier, role)['txn']['data']))

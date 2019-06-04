import pytest as pytest

from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE, \
    TRUSTEE, DOMAIN_LEDGER_ID, TXN_AUTHOR_AGREEMENT_AML, \
    AML_VERSION, AML, AML_CONTEXT, GET_TXN_AUTHOR_AGREEMENT_AML, GET_TXN_AUTHOR_AGREEMENT_AML_VERSION, \
    GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.get_txn_author_agreement_aml_handler import GetTxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
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
def get_txn_author_agreement_aml_handler(tconf, domain_state):
    data_manager = DatabaseManager()
    handler = GetTxnAuthorAgreementAmlHandler(data_manager, FakeSomething())
    return handler


def test_static_validation(get_txn_author_agreement_aml_handler):
    request = Request(operation={TXN_TYPE: GET_TXN_AUTHOR_AGREEMENT_AML,
                                 GET_TXN_AUTHOR_AGREEMENT_AML_VERSION: "VERSION"})
    get_txn_author_agreement_aml_handler.static_validation(request)

    request = Request(operation={TXN_TYPE: GET_TXN_AUTHOR_AGREEMENT_AML,
                                 GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP: 1559299045})
    get_txn_author_agreement_aml_handler.static_validation(request)


def test_static_validation_with_redundant_fields(get_txn_author_agreement_aml_handler):
    request = Request(operation={TXN_TYPE: GET_TXN_AUTHOR_AGREEMENT_AML,
                                 GET_TXN_AUTHOR_AGREEMENT_AML_VERSION: "VERSION",
                                 GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP: 1559299045})
    with pytest.raises(InvalidClientRequest,
                       match='"version" and "timestamp" cannot be used in '
                             'GET_TXN_AUTHOR_AGREEMENT_AML request together'):
        get_txn_author_agreement_aml_handler.static_validation(request)


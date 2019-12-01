import pytest

from common.serializers.serialization import config_state_serializer
from plenum.server.request_handlers.utils import encode_state_value

from plenum.common.constants import TRUSTEE, TXN_TYPE, TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_TEXT, \
    TXN_AUTHOR_AGREEMENT_VERSION, DOMAIN_LEDGER_ID, TXN_AUTHOR_AGREEMENT_RETIRED
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.txn_author_agreement_handler import TxnAuthorAgreementHandler

from plenum.test.req_handler.helper import update_nym
from plenum.test.testing_utils import FakeSomething
from state.state import State


@pytest.fixture(scope="function")
def domain_state(tconf):
    state = State()
    state.txn_list = {}
    state.get = lambda key, isCommitted=False: state.txn_list.get(key, None)
    state.set = lambda key, value, isCommitted=False: state.txn_list.update({key: value})
    return state


@pytest.fixture(scope="function", params=[True, False, None])
def taa_request(tconf, domain_state, request):
    identifier = "identifier"
    update_nym(domain_state, identifier, TRUSTEE)
    operation = {TXN_TYPE: TXN_AUTHOR_AGREEMENT,
                              TXN_AUTHOR_AGREEMENT_TEXT: "text",
                              TXN_AUTHOR_AGREEMENT_VERSION: "version"}
    if request.param is not None:
        operation[TXN_AUTHOR_AGREEMENT_RETIRED] = request.param
    return Request(identifier=identifier,
                   signature="sign",
                   operation=operation)

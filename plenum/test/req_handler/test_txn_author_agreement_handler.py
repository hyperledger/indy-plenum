import pytest as pytest

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE, TXN_AUTHOR_AGREEMENT, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, TRUSTEE, DOMAIN_LEDGER_ID
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, get_payload_data, append_txn_metadata
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.txn_author_agreement_handler import TxnAuthorAgreementHandler
from plenum.server.request_handlers.utils import nym_to_state_key, encode_state_value
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
    txn_author_agreement_handler.state.set(StaticTAAHelper.state_path_taa_aml_latest(),
                                           encode_state_value("value", "seqNo", "txnTime",
                                                              serializer=config_state_serializer))


@pytest.fixture(scope="function")
def taa_request(tconf, txn_author_agreement_handler, domain_state):
    identifier = "identifier"
    update_nym(domain_state, identifier, TRUSTEE)
    return Request(identifier=identifier,
                   signature="sign",
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


def test_update_state(txn_author_agreement_handler, taa_request):
    seq_no = 1
    txn_time = 1560241033
    txn_id = "id"
    txn = reqToTxn(taa_request)
    payload = get_payload_data(txn)
    text = payload[TXN_AUTHOR_AGREEMENT_TEXT]
    version = payload[TXN_AUTHOR_AGREEMENT_VERSION]
    digest = StaticTAAHelper.taa_digest(text, version)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)

    txn_author_agreement_handler.update_state(txn, None, taa_request)

    assert txn_author_agreement_handler.get_from_state(
        StaticTAAHelper.state_path_taa_digest(digest)) == (payload, seq_no, txn_time)
    assert txn_author_agreement_handler.state.get(
        StaticTAAHelper.state_path_taa_latest()) == digest
    assert txn_author_agreement_handler.state.get(
        StaticTAAHelper.state_path_taa_version(version)) == digest

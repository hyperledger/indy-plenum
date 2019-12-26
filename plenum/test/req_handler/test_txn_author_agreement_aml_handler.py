import pytest as pytest

from common.serializers.serialization import domain_state_serializer, config_state_serializer
from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE, \
    TRUSTEE, DOMAIN_LEDGER_ID, TXN_AUTHOR_AGREEMENT_AML, \
    AML_VERSION, AML, AML_CONTEXT
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, append_txn_metadata, get_payload_data
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.txn_author_agreement_aml_handler import TxnAuthorAgreementAmlHandler
from plenum.server.request_handlers.utils import nym_to_state_key, encode_state_value
from plenum.test.req_handler.helper import update_nym
from plenum.test.testing_utils import FakeSomething
from state.pruning_state import PruningState
from state.state import State
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture(scope="function")
def domain_state(tconf):
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture(scope="function")
def txn_author_agreement_aml_handler(tconf, domain_state):
    data_manager = DatabaseManager()
    handler = TxnAuthorAgreementAmlHandler(data_manager)
    state = PruningState(KeyValueStorageInMemory())
    data_manager.register_new_database(handler.ledger_id,
                                       FakeSomething(),
                                       state)
    data_manager.register_new_database(DOMAIN_LEDGER_ID,
                                       FakeSomething(),
                                       domain_state)
    return handler


@pytest.fixture(scope="function")
def aml_request(tconf, txn_author_agreement_aml_handler, domain_state):
    identifier = "identifier"
    update_nym(domain_state, identifier, TRUSTEE)
    return Request(identifier=identifier,
                   operation={TXN_TYPE: TXN_AUTHOR_AGREEMENT_AML,
                              AML_VERSION: "AML_VERSION",
                              AML: {"test": "test"},
                              AML_CONTEXT: "AML_CONTEXT"})


def test_static_validation(txn_author_agreement_aml_handler, aml_request):
    txn_author_agreement_aml_handler.static_validation(aml_request)


def test_static_validation_with_empty_aml(txn_author_agreement_aml_handler, aml_request):
    aml_request.operation[AML] = {}
    with pytest.raises(InvalidClientRequest,
                       match="TXN_AUTHOR_AGREEMENT_AML request must contain at least one acceptance mechanism"):
        txn_author_agreement_aml_handler.static_validation(aml_request)


def test_dynamic_validation(txn_author_agreement_aml_handler, aml_request):
    txn_author_agreement_aml_handler.dynamic_validation(aml_request, 0)


def test_dynamic_validation_with_not_unique_aml(txn_author_agreement_aml_handler, aml_request):
    version = aml_request.operation[AML_VERSION]
    txn_author_agreement_aml_handler.state.set(StaticTAAHelper.state_path_taa_aml_version(version), "{}")
    with pytest.raises(InvalidClientRequest,
                       match="Version of TAA AML must be unique and it cannot be modified"):
        txn_author_agreement_aml_handler.dynamic_validation(aml_request, 0)


def test_dynamic_validation_from_steward(txn_author_agreement_aml_handler, domain_state, aml_request):
    identifier = "test_identifier"
    update_nym(domain_state, identifier, STEWARD)
    aml_request._identifier = identifier
    with pytest.raises(UnauthorizedClientRequest,
                       match="Only trustee can update transaction author agreement and AML"):
        txn_author_agreement_aml_handler.dynamic_validation(aml_request, 0)


def test_update_state(txn_author_agreement_aml_handler, domain_state, aml_request):
    seq_no = 1
    txn_time = 1560241033
    txn_id = "id"
    txn = reqToTxn(aml_request)
    payload = get_payload_data(txn)
    version = payload[AML_VERSION]
    append_txn_metadata(txn, seq_no, txn_time, txn_id)

    txn_author_agreement_aml_handler.update_state(txn, None, aml_request)

    assert txn_author_agreement_aml_handler.get_from_state(
        StaticTAAHelper.state_path_taa_aml_latest()) == (payload, seq_no, txn_time)
    assert txn_author_agreement_aml_handler.get_from_state(
        StaticTAAHelper.state_path_taa_aml_version(version)) == (payload, seq_no, txn_time)

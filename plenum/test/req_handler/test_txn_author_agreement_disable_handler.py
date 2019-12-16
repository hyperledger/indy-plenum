import pytest as pytest

from plenum.server.request_handlers.txn_author_agreement_disable_handler import TxnAuthorAgreementDisableHandler
from storage.kv_in_memory import KeyValueStorageInMemory

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE, TXN_AUTHOR_AGREEMENT, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, TRUSTEE, DOMAIN_LEDGER_ID, TXN_AUTHOR_AGREEMENT_DIGEST, \
    TXN_AUTHOR_AGREEMENT_RETIRED, TXN_AUTHOR_AGREEMENT_TIMESTAMP, TXN_METADATA, TXN_METADATA_TIME, \
    TXN_AUTHOR_AGREEMENT_DISABLE
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, get_payload_data, append_txn_metadata
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.txn_author_agreement_handler import TxnAuthorAgreementHandler
from plenum.server.request_handlers.utils import nym_to_state_key, encode_state_value
from plenum.test.req_handler.helper import update_nym
from plenum.test.testing_utils import FakeSomething
from state.pruning_state import PruningState
from state.state import State


@pytest.fixture(scope="function")
def txn_author_agreement_disable_handler(tconf, domain_state, config_state):
    data_manager = DatabaseManager()
    handler = TxnAuthorAgreementDisableHandler(data_manager)
    data_manager.register_new_database(handler.ledger_id,
                                       FakeSomething(),
                                       config_state)
    data_manager.register_new_database(DOMAIN_LEDGER_ID,
                                       FakeSomething(),
                                       domain_state)
    return handler


@pytest.fixture(scope="function")
def taa_disable_request(tconf, domain_state):
    identifier = "identifier"
    update_nym(domain_state, identifier, TRUSTEE)
    operation = {TXN_TYPE: TXN_AUTHOR_AGREEMENT_DISABLE}
    return Request(identifier=identifier,
                   signature="sign",
                   operation=operation)


def test_static_validation(txn_author_agreement_disable_handler, taa_disable_request):
    txn_author_agreement_disable_handler.static_validation(taa_disable_request)


def test_dynamic_validation(txn_author_agreement_disable_handler, taa_disable_request):
    txn_author_agreement_disable_handler.state.set(StaticTAAHelper.state_path_taa_latest(), "{}")
    txn_author_agreement_disable_handler.dynamic_validation(taa_disable_request)


def test_dynamic_validation_for_already_disable_taa(txn_author_agreement_disable_handler, taa_disable_request):
    with pytest.raises(InvalidClientRequest,
                       match="Transaction author agreement is already disabled."):
        txn_author_agreement_disable_handler.dynamic_validation(taa_disable_request)


def test_update_state(txn_author_agreement_disable_handler, taa_request,
                      taa_disable_request, txn_author_agreement_handler):
    taa_seq_no = 1
    disable_seq_no = taa_seq_no + 1
    taa_txn_time = 1560241033
    disable_txn_time = taa_txn_time + 1
    txn_id = "id"
    taa_txn = reqToTxn(taa_request)
    taa_disable_txn = reqToTxn(taa_disable_request)
    payload = get_payload_data(taa_txn)
    text = payload[TXN_AUTHOR_AGREEMENT_TEXT]
    version = payload[TXN_AUTHOR_AGREEMENT_VERSION]
    retired = payload.get(TXN_AUTHOR_AGREEMENT_RETIRED, False)
    digest = StaticTAAHelper.taa_digest(text, version)
    append_txn_metadata(taa_txn, taa_seq_no, taa_txn_time, txn_id)
    append_txn_metadata(taa_disable_txn, disable_seq_no, disable_txn_time, txn_id)

    state_value = {TXN_AUTHOR_AGREEMENT_TEXT: text,
                   TXN_AUTHOR_AGREEMENT_VERSION: version,
                   TXN_AUTHOR_AGREEMENT_DIGEST: digest}
    if retired:
        state_value[TXN_AUTHOR_AGREEMENT_RETIRED] = retired
    state_value[TXN_AUTHOR_AGREEMENT_TIMESTAMP] = None if retired else taa_txn_time

    # Set a TAA
    txn_author_agreement_handler.update_state(taa_txn, None, taa_request)

    assert txn_author_agreement_disable_handler.get_from_state(
        StaticTAAHelper.state_path_taa_digest(digest)) == (state_value, taa_seq_no, taa_txn_time)
    assert txn_author_agreement_disable_handler.state.get(
        StaticTAAHelper.state_path_taa_latest(), isCommitted=False) == digest.encode()
    assert txn_author_agreement_disable_handler.state.get(
        StaticTAAHelper.state_path_taa_version(version), isCommitted=False) == digest.encode()

    # Disable TAA
    txn_author_agreement_disable_handler.update_state(taa_disable_txn, None, taa_disable_request)

    state_value[TXN_AUTHOR_AGREEMENT_RETIRED] = txn_author_agreement_disable_handler.retired_time

    print(txn_author_agreement_handler.get_from_state(
        StaticTAAHelper.state_path_taa_digest(digest)))
    print((state_value, disable_seq_no, disable_txn_time))


    assert txn_author_agreement_disable_handler.get_from_state(
        StaticTAAHelper.state_path_taa_digest(digest)) == (state_value, disable_seq_no, disable_txn_time)
    assert txn_author_agreement_disable_handler.state.get(
        StaticTAAHelper.state_path_taa_latest(), isCommitted=False) is None
    assert txn_author_agreement_disable_handler.state.get(
        StaticTAAHelper.state_path_taa_version(version), isCommitted=False) == digest.encode()

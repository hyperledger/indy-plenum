import pytest as pytest

from plenum.common.util import get_utc_epoch
from storage.kv_in_memory import KeyValueStorageInMemory

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE, TXN_AUTHOR_AGREEMENT, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, TRUSTEE, DOMAIN_LEDGER_ID, TXN_AUTHOR_AGREEMENT_DIGEST, \
    TXN_AUTHOR_AGREEMENT_RETIRED, TXN_AUTHOR_AGREEMENT_RATIFIED, TXN_METADATA, TXN_METADATA_TIME
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, get_payload_data, append_txn_metadata
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.txn_author_agreement_handler import TxnAuthorAgreementHandler
from plenum.server.request_handlers.utils import nym_to_state_key, encode_state_value
from plenum.test.req_handler.helper import update_nym, create_taa_txn, check_taa_in_state
from plenum.test.testing_utils import FakeSomething
from state.pruning_state import PruningState
from state.state import State


@pytest.fixture(scope="function")
def set_aml(txn_author_agreement_handler):
    txn_author_agreement_handler.state.set(StaticTAAHelper.state_path_taa_aml_latest(),
                                           encode_state_value("value", "seqNo", "txnTime",
                                                              serializer=config_state_serializer))


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
    txn = reqToTxn(taa_request)
    txn_author_agreement_handler.update_state(txn, None, taa_request)
    txn_author_agreement_handler.dynamic_validation(taa_request)
    taa_request.operation[TXN_AUTHOR_AGREEMENT_TEXT] = ""
    with pytest.raises(InvalidClientRequest,
                       match="Changing a text of existing transaction author agreement is forbidden"):
        txn_author_agreement_handler.dynamic_validation(taa_request)


def test_update_state(txn_author_agreement_handler, taa_request):
    txn, digest, state_data = create_taa_txn(taa_request)

    txn_author_agreement_handler.update_state(txn, None, taa_request)

    check_taa_in_state(handler=txn_author_agreement_handler,
                       digest=digest,
                       version=state_data[0][TXN_AUTHOR_AGREEMENT_VERSION],
                       state_data=state_data)
    assert txn_author_agreement_handler.state.get(
        StaticTAAHelper.state_path_taa_latest(), isCommitted=False) == digest.encode()


@pytest.mark.parametrize("retired_time", [get_utc_epoch(), None])
def test_update_state_one_by_one(txn_author_agreement_handler, taa_request, retired_time):
    txn, digest, state_data = create_taa_txn(taa_request)
    state_value, seq_no, txn_time_first = state_data
    payload = get_payload_data(txn)
    txn_time_second = get_utc_epoch()

    # update state
    txn_author_agreement_handler.update_state(txn, None, None)
    payload[TXN_AUTHOR_AGREEMENT_RETIRED] = retired_time
    txn[TXN_METADATA][TXN_METADATA_TIME] = txn_time_second
    txn_author_agreement_handler.update_state(txn, None, None)

    if retired_time:
        state_value[TXN_AUTHOR_AGREEMENT_RETIRED] = retired_time

    assert txn_author_agreement_handler.get_from_state(
        StaticTAAHelper.state_path_taa_digest(digest)) == (state_value, seq_no, txn_time_second)

import pytest as pytest

from plenum.common.util import get_utc_epoch
from plenum.server.request_handlers.txn_author_agreement_disable_handler import TxnAuthorAgreementDisableHandler
from plenum.test.req_handler.conftest import taa_request
from storage.kv_in_memory import KeyValueStorageInMemory

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import ROLE, STEWARD, NYM, TARGET_NYM, TXN_TYPE, TXN_AUTHOR_AGREEMENT, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, TRUSTEE, DOMAIN_LEDGER_ID, TXN_AUTHOR_AGREEMENT_DIGEST, \
    TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, TXN_METADATA, TXN_METADATA_TIME, \
    TXN_AUTHOR_AGREEMENT_DISABLE
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
    txn_author_agreement_disable_handler.dynamic_validation(taa_disable_request, 0)


def test_dynamic_validation_for_already_disable_taa(txn_author_agreement_disable_handler, taa_disable_request):
    with pytest.raises(InvalidClientRequest,
                       match="Transaction author agreement is already disabled."):
        txn_author_agreement_disable_handler.dynamic_validation(taa_disable_request, 0)


def test_update_state(txn_author_agreement_disable_handler,
                      taa_disable_request, txn_author_agreement_handler, tconf, domain_state, taa_pp_time):
    # create TAAs
    taa_txns = []
    taa_digests = []
    taa_state_datas = []
    for _ in list(range(5)):
        txn, digest, state_data = create_taa_txn(taa_request(tconf, domain_state, taa_pp_time), taa_pp_time)
        taa_txns.append(txn)
        taa_digests.append(digest)
        taa_state_datas.append(state_data)
    assert taa_txns

    # create a disable txn
    disable_seq_no = 1
    disable_txn_time = get_utc_epoch()
    taa_disable_txn = reqToTxn(taa_disable_request)
    append_txn_metadata(taa_disable_txn, disable_seq_no, disable_txn_time)

    # set a TAAs
    for index, taa_txn in enumerate(taa_txns):
        txn_author_agreement_handler.update_state(taa_txn, None, None)

        check_taa_in_state(handler=txn_author_agreement_handler,
                           digest=taa_digests[index],
                           version=taa_state_datas[index][0][TXN_AUTHOR_AGREEMENT_VERSION],
                           state_data=taa_state_datas[index])
        assert txn_author_agreement_disable_handler.state.get(
            StaticTAAHelper.state_path_taa_latest(), isCommitted=False) == taa_digests[index].encode()

    # disable TAAs
    txn_author_agreement_disable_handler.update_state(taa_disable_txn, None, None)

    assert txn_author_agreement_disable_handler.state.get(
        StaticTAAHelper.state_path_taa_latest(), isCommitted=False) is None

    # set a TAAs
    for index, state_data in enumerate(taa_state_datas):
        state_value = state_data[0]
        state_value[TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] = disable_txn_time
        check_taa_in_state(handler=txn_author_agreement_handler,
                           digest=taa_digests[index],
                           version=state_value[TXN_AUTHOR_AGREEMENT_VERSION],
                           state_data=(state_data[0], disable_seq_no, disable_txn_time))

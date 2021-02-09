import pytest as pytest

from plenum.common.constants import STEWARD, TXN_TYPE, \
    TRUSTEE, DOMAIN_LEDGER_ID, LEDGERS_FREEZE, LEDGERS_IDS, VALID_LEDGER_IDS, AUDIT_LEDGER_ID, TXN_PAYLOAD, \
    TXN_PAYLOAD_DATA, AUDIT_TXN_LEDGERS_SIZE, AUDIT_TXN_LEDGER_ROOT, AUDIT_TXN_STATE_ROOT
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, append_txn_metadata, get_payload_data
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.ledgers_freeze.ledger_freeze_helper import StaticLedgersFreezeHelper
from plenum.server.request_handlers.ledgers_freeze.ledgers_freeze_handler import LedgersFreezeHandler
from plenum.test.req_handler.helper import update_nym
from plenum.test.testing_utils import FakeSomething
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture(scope="function")
def audit_ledger(tconf):
    return FakeSomething(size=0, get_last_committed_txn=lambda *args: None, getAllTxn=lambda *args, **kwargs: [])


@pytest.fixture(scope="function")
def ledgers_freeze_handler(tconf, domain_state, audit_ledger):
    data_manager = DatabaseManager()
    handler = LedgersFreezeHandler(data_manager)
    state = PruningState(KeyValueStorageInMemory())
    data_manager.register_new_database(handler.ledger_id,
                                       FakeSomething(),
                                       state)
    data_manager.register_new_database(DOMAIN_LEDGER_ID,
                                       FakeSomething(),
                                       domain_state)
    data_manager.register_new_database(AUDIT_LEDGER_ID, audit_ledger)
    return handler


@pytest.fixture(scope="function")
def ledgers_freeze_request(tconf, ledgers_freeze_handler, domain_state):
    identifier = "identifier"
    update_nym(domain_state, identifier, TRUSTEE)
    return Request(identifier=identifier,
                   operation={TXN_TYPE: LEDGERS_FREEZE,
                              LEDGERS_IDS: [909]})


def update_fake_audit_ledger(audit_ledger, ledger_sizes, ledger_roots={}, state_roots={}):
    txn = {TXN_PAYLOAD: {TXN_PAYLOAD_DATA: {AUDIT_TXN_LEDGERS_SIZE: ledger_sizes,
                                            AUDIT_TXN_LEDGER_ROOT: ledger_roots,
                                            AUDIT_TXN_STATE_ROOT: state_roots}}}
    audit_ledger.get_last_committed_txn = lambda *args: txn


def test_static_validation(ledgers_freeze_handler, ledgers_freeze_request):
    ledgers_freeze_handler.static_validation(ledgers_freeze_request)


def test_static_validation_with_incorrect_ledger_id(ledgers_freeze_handler, ledgers_freeze_request):
    ledgers_freeze_request.operation[LEDGERS_IDS] = [DOMAIN_LEDGER_ID]
    with pytest.raises(InvalidClientRequest,
                       match="ledgers can't be frozen"):
        ledgers_freeze_handler.static_validation(ledgers_freeze_request)


def test_dynamic_validation(ledgers_freeze_handler, ledgers_freeze_request, audit_ledger):
    update_fake_audit_ledger(audit_ledger, ledgers_freeze_request.operation.get(LEDGERS_IDS))
    ledgers_freeze_handler.dynamic_validation(ledgers_freeze_request, 0)


def test_dynamic_validation_with_incorrect_ledger_id(ledgers_freeze_handler, ledgers_freeze_request, audit_ledger):
    update_fake_audit_ledger(audit_ledger, {})
    with pytest.raises(InvalidClientRequest,
                       match="One or more ledgers from .* "
                             "have never existed"):
        ledgers_freeze_handler.dynamic_validation(ledgers_freeze_request, 0)


def test_dynamic_validation_from_steward(ledgers_freeze_handler, domain_state, ledgers_freeze_request):
    identifier = "test_identifier"
    update_nym(domain_state, identifier, STEWARD)
    ledgers_freeze_request._identifier = identifier
    with pytest.raises(UnauthorizedClientRequest,
                       match="Only trustee can freeze ledgers"):
        ledgers_freeze_handler.dynamic_validation(ledgers_freeze_request, 0)


def create_txn(ledgers_freeze_request, seq_no, txn_time):
    txn_id = "id"
    txn = reqToTxn(ledgers_freeze_request)
    append_txn_metadata(txn, seq_no, txn_time, txn_id)
    return txn


def test_update_state(ledgers_freeze_handler, domain_state, ledgers_freeze_request, audit_ledger):
    seq_no = 1
    txn_time = 1560241033
    txn = create_txn(ledgers_freeze_request, seq_no, txn_time)
    ledgers_ids = get_payload_data(txn)[LEDGERS_IDS]
    update_fake_audit_ledger(audit_ledger,
                             ledger_sizes={lid: 0 for lid in ledgers_ids},
                             ledger_roots={lid: "ledger_root" for lid in ledgers_ids},
                             state_roots={lid: "state_root" for lid in ledgers_ids})

    ledgers_freeze_handler.update_state(txn, None, ledgers_freeze_request)
    assert ledgers_freeze_handler.get_from_state(
        StaticLedgersFreezeHelper.make_state_path_for_frozen_ledgers()) == (
               {str(ledgers_ids[0]): {'state': 'state_root',
                                      'seq_no': 0,
                                      'ledger': 'ledger_root'}},
               seq_no, txn_time)


def test_update_state_with_empty_request(ledgers_freeze_handler, domain_state, ledgers_freeze_request, audit_ledger):
    seq_no = 1
    txn_time = 1560241033
    txn1 = create_txn(ledgers_freeze_request, seq_no, txn_time)
    ledgers_ids = get_payload_data(txn1)[LEDGERS_IDS]

    seq_no_2 = 2
    txn_time_2 = 1560241034
    ledgers_freeze_request.operation[LEDGERS_IDS] = []
    txn_2 = create_txn(ledgers_freeze_request, seq_no_2, txn_time_2)
    update_fake_audit_ledger(audit_ledger,
                             ledger_sizes={lid: 0 for lid in ledgers_ids},
                             ledger_roots={lid: "ledger_root" for lid in ledgers_ids},
                             state_roots={lid: "state_root" for lid in ledgers_ids})

    ledgers_freeze_handler.update_state(txn1, None, ledgers_freeze_request)
    ledgers_freeze_handler.update_state(txn_2, None, ledgers_freeze_request)
    assert ledgers_freeze_handler.get_from_state(
        StaticLedgersFreezeHelper.make_state_path_for_frozen_ledgers()) == (
               {str(ledgers_ids[0]): {'state': 'state_root',
                                      'seq_no': 0,
                                      'ledger': 'ledger_root'}},
               seq_no_2, txn_time_2)


def test_update_state_with_adding_ledger_id(ledgers_freeze_handler, domain_state, ledgers_freeze_request, audit_ledger):
    ledgers_ids = []
    seq_no_1 = 1
    txn_time_1 = 1560241033
    txn_1 = create_txn(ledgers_freeze_request, seq_no_1, txn_time_1)
    ledgers_ids += get_payload_data(txn_1)[LEDGERS_IDS]

    seq_no_2 = 2
    txn_time_2 = 1560241034
    ledgers_freeze_request.operation[LEDGERS_IDS] = [888]
    txn_2 = create_txn(ledgers_freeze_request, seq_no_2, txn_time_2)
    ledgers_ids += get_payload_data(txn_2)[LEDGERS_IDS]

    update_fake_audit_ledger(audit_ledger,
                             ledger_sizes={lid: 0 for lid in ledgers_ids},
                             ledger_roots={lid: "ledger_root" for lid in ledgers_ids},
                             state_roots={lid: "state_root" for lid in ledgers_ids})

    ledgers_freeze_handler.update_state(txn_1, None, ledgers_freeze_request)
    ledgers_freeze_handler.update_state(txn_2, None, ledgers_freeze_request)

    assert len(ledgers_ids) == 2
    assert ledgers_freeze_handler.get_from_state(
        StaticLedgersFreezeHelper.make_state_path_for_frozen_ledgers()) == (
               {str(lid): {'state': 'state_root',
                           'seq_no': 0,
                           'ledger': 'ledger_root'} for lid in ledgers_ids},
               seq_no_2, txn_time_2)

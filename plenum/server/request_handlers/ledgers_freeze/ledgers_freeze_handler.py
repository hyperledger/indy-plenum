from typing import Optional

from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import CONFIG_LEDGER_ID, AUDIT_LEDGER_ID, AUDIT_TXN_LEDGER_ROOT, \
    AUDIT_TXN_STATE_ROOT, AUDIT_TXN_LEDGERS_SIZE, LEDGERS_FREEZE, LEDGERS_IDS, DOMAIN_LEDGER_ID, VALID_LEDGER_IDS
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_seq_no, get_txn_time
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.ledgers_freeze.ledger_freeze_helper import StaticLedgersFreezeHelper
from plenum.server.request_handlers.utils import encode_state_value, is_trustee


class LedgersFreezeHandler(WriteRequestHandler):
    state_serializer = config_state_serializer

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, LEDGERS_FREEZE, CONFIG_LEDGER_ID)

    def static_validation(self, request: Request):
        self._validate_request_type(request)
        if any(lid in VALID_LEDGER_IDS for lid in request.operation.get(LEDGERS_IDS)):
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "{} ledgers can't be frozen".format(VALID_LEDGER_IDS))

    def additional_dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        audit_ledger = self.database_manager.get_ledger(AUDIT_LEDGER_ID)
        if not audit_ledger:
            return
        last_txn = audit_ledger.get_last_committed_txn()
        if any(lid not in get_payload_data(last_txn).get(AUDIT_TXN_LEDGERS_SIZE)
               for lid in request.operation.get(LEDGERS_IDS)):
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "One or more ledgers from {} have never existed".format(request.operation.get(LEDGERS_IDS)))

    def authorize(self, request):
        domain_state = self.database_manager.get_database(DOMAIN_LEDGER_ID).state
        if not is_trustee(domain_state, request.identifier, is_committed=False):
            raise UnauthorizedClientRequest(request.identifier, request.reqId,
                                            "Only trustee can freeze ledgers")

    def update_state(self, txn, prev_result, request, is_committed=False):
        self._validate_txn_type(txn)
        seq_no = get_seq_no(txn)
        txn_time = get_txn_time(txn)
        ledgers_ids = get_payload_data(txn)[LEDGERS_IDS]
        frozen_ledgers = self.make_frozen_ledgers_list(ledgers_ids)
        self.state.set(StaticLedgersFreezeHelper.make_state_path_for_frozen_ledgers(),
                       encode_state_value(frozen_ledgers, seq_no, txn_time))
        return txn

    def make_frozen_ledgers_list(self, ledgers_ids):
        audit_ledger = self.database_manager.get_ledger(AUDIT_LEDGER_ID)
        last_audit_txn = audit_ledger.get_last_committed_txn()

        config_state = self.database_manager.get_state(CONFIG_LEDGER_ID)
        frozen_ledgers_list = StaticLedgersFreezeHelper.get_frozen_ledgers(config_state, is_committed=False)
        for ledger_id in ledgers_ids:
            if ledger_id in frozen_ledgers_list:
                continue
            ledger_root, state_root, seq_no = self._load_hash_roots_from_audit_ledger(audit_ledger,
                                                                                      last_audit_txn,
                                                                                      ledger_id)
            frozen_ledgers_list[ledger_id] = StaticLedgersFreezeHelper.create_frozen_ledger_info(ledger_root,
                                                                                                 state_root,
                                                                                                 seq_no)
        return frozen_ledgers_list

    @staticmethod
    def _load_hash_roots_from_audit_ledger(audit_ledger, last_audit_txn, ledger_id):
        last_txn_ledger_root = get_payload_data(last_audit_txn).get(AUDIT_TXN_LEDGER_ROOT).get(ledger_id, None)
        last_txn_state_root = get_payload_data(last_audit_txn).get(AUDIT_TXN_STATE_ROOT).get(ledger_id, None)
        last_txn_seq_no = get_payload_data(last_audit_txn).get(AUDIT_TXN_LEDGERS_SIZE).get(ledger_id, None)

        # If the current ledger root hash value is integer, it means that ledger root wasn't changed
        # for the last `value` audit txns. And we get last audit transaction with string root hash.
        if isinstance(last_txn_ledger_root, int):
            seq_no = get_seq_no(last_audit_txn) - last_txn_ledger_root
            audit_txn_for_seq_no = audit_ledger.getBySeqNo(seq_no)
            last_txn_ledger_root = get_payload_data(audit_txn_for_seq_no).get(AUDIT_TXN_LEDGER_ROOT).get(ledger_id, None)

        if isinstance(last_txn_state_root, int):
            seq_no = get_seq_no(last_audit_txn) - last_txn_ledger_root
            audit_txn_for_seq_no = audit_ledger.getBySeqNo(seq_no)
            last_txn_state_root = get_payload_data(audit_txn_for_seq_no).get(AUDIT_TXN_STATE_ROOT).get(ledger_id, None)

        return last_txn_ledger_root, last_txn_state_root, last_txn_seq_no

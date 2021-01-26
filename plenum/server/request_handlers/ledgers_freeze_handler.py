from typing import Optional

from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import CONFIG_LEDGER_ID, AUDIT_LEDGER_ID, AUDIT_TXN_LEDGER_ROOT, \
    AUDIT_TXN_STATE_ROOT, AUDIT_TXN_LEDGERS_SIZE, LEDGERS_FREEZE, LEDGERS_IDS, DOMAIN_LEDGER_ID, VALID_LEDGER_IDS
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_seq_no, get_txn_time
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.state_constants import MARKER_FROZEN_LEDGERS
from plenum.server.request_handlers.utils import encode_state_value, is_trustee


class LedgersFreezeHandler(WriteRequestHandler):
    state_serializer = config_state_serializer

    LEDGER = "ledger"
    STATE = "state"
    SEQ_NO = "seq_no"

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, LEDGERS_FREEZE, CONFIG_LEDGER_ID)

    def static_validation(self, request: Request):
        self._validate_request_type(request)
        if any(lid in VALID_LEDGER_IDS for lid in request.operation.get(LEDGERS_IDS)):
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "{} ledgers can't be frozen".format(VALID_LEDGER_IDS))

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        super().dynamic_validation(request, req_pp_time)
        # TODO: add a check for existing ledgers_ids in audit

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
        self.state.set(self.make_state_path_for_frozen_ledgers(), encode_state_value(frozen_ledgers, seq_no, txn_time))
        return txn

    @staticmethod
    def make_state_path_for_frozen_ledgers() -> bytes:
        return "{MARKER}:FROZEN_LEDGERS" \
            .format(MARKER=MARKER_FROZEN_LEDGERS).encode()

    def make_frozen_ledgers_list(self, ledgers_ids):
        ledger_root, state_root, seq_no = self.__load_hash_roots_from_audit_ledger()
        return {ledger_id: {LedgersFreezeHandler.LEDGER: ledger_root,
                            LedgersFreezeHandler.STATE: state_root,
                            LedgersFreezeHandler.SEQ_NO: seq_no} for ledger_id in ledgers_ids}

    def __load_hash_roots_from_audit_ledger(self):
        audit_ledger = self.database_manager.get_ledger(AUDIT_LEDGER_ID)
        if not audit_ledger:
            return None, None, None

        last_txn = audit_ledger.get_last_committed_txn()
        last_txn_ledger_root = get_payload_data(last_txn).get(AUDIT_TXN_LEDGER_ROOT, None)
        last_txn_state_root = get_payload_data(last_txn).get(AUDIT_TXN_STATE_ROOT, None)
        last_txn_seq_no = get_payload_data(last_txn).get(AUDIT_TXN_LEDGERS_SIZE, None)

        if isinstance(last_txn_ledger_root, int):
            seq_no = get_seq_no(last_txn) - last_txn_ledger_root
            audit_txn_for_seq_no = audit_ledger.getBySeqNo(seq_no)
            last_txn_ledger_root = get_payload_data(audit_txn_for_seq_no).get(AUDIT_TXN_LEDGER_ROOT)

        if isinstance(last_txn_state_root, int):
            seq_no = get_seq_no(last_txn) - last_txn_ledger_root
            audit_txn_for_seq_no = audit_ledger.getBySeqNo(seq_no)
            last_txn_state_root = get_payload_data(audit_txn_for_seq_no).get(AUDIT_TXN_STATE_ROOT)

        return last_txn_ledger_root, last_txn_state_root, last_txn_seq_no

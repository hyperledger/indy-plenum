from datetime import datetime
from typing import Optional

from common.exceptions import LogicError
from common.serializers.serialization import config_state_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID, TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, TXN_AUTHOR_AGREEMENT_RATIFICATION_TS
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_seq_no, get_txn_time
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.base_taa_handler import BaseTAAHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.utils import decode_state_value


class TxnAuthorAgreementHandler(BaseTAAHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID)

    def static_validation(self, request: Request):
        self._validate_request_type(request)
        operation, identifier, req_id = request.operation, request.identifier, request.reqId
        self._validate_ts(operation.get(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS),
                          identifier, req_id, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS)
        self._validate_ts(operation.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS),
                          identifier, req_id, TXN_AUTHOR_AGREEMENT_RATIFICATION_TS)

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        self._validate_request_type(request)
        self.authorize(request)
        operation, identifier, req_id = request.operation, request.identifier, request.reqId
        aml_latest, _, _ = self.get_from_state(StaticTAAHelper.state_path_taa_aml_latest())
        if aml_latest is None:
            raise InvalidClientRequest(identifier, req_id,
                                       "TAA txn is forbidden until TAA AML is set. Send TAA AML first.")
        version = operation[TXN_AUTHOR_AGREEMENT_VERSION]
        digest = StaticTAAHelper.get_taa_digest(self.state, version, isCommitted=False)
        if digest is None:
            if req_pp_time is None:
                raise LogicError("Cannot validate TAA transaction outside of normal ordering")
            self._validate_add_taa(request, req_pp_time)
        else:
            self._validate_update_taa(request, digest)

    def update_state(self, txn, prev_result, request, is_committed=False):
        self._validate_txn_type(txn)
        payload = get_payload_data(txn)
        text = payload.get(TXN_AUTHOR_AGREEMENT_TEXT)
        version = payload[TXN_AUTHOR_AGREEMENT_VERSION]
        retired = payload.get(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS)
        ratified = payload.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS)
        seq_no = get_seq_no(txn)
        txn_time = get_txn_time(txn)
        digest = StaticTAAHelper.get_taa_digest(self.state, version, isCommitted=False)
        if digest is None:
            digest = StaticTAAHelper.taa_digest(text, version)
            self._add_taa_to_state(digest, seq_no, txn_time, text, version, ratified)
        else:
            self._update_taa_to_state(digest, seq_no, txn_time, retired)

    def authorize(self, request):
        StaticTAAHelper.authorize(self.database_manager, request)

    def _add_taa_to_state(self, digest, seq_no, txn_time, text, version, ratification_ts):
        self._set_taa_to_state(digest, seq_no, txn_time, text, version,
                               ratification_ts)

        self.state.set(StaticTAAHelper.state_path_taa_version(version), digest)
        self.state.set(StaticTAAHelper.state_path_taa_latest(), digest)

    def _update_taa_to_state(self, digest, seq_no, txn_time, retirement_ts=None):
        ledger_data = self.get_from_state(StaticTAAHelper.state_path_taa_digest(digest))
        if ledger_data is None:
            return
        ledger_taa, last_seq_no, last_update_time = ledger_data
        ratification_ts = ledger_taa.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, last_update_time)
        text = ledger_taa.get(TXN_AUTHOR_AGREEMENT_TEXT)
        version = ledger_taa.get(TXN_AUTHOR_AGREEMENT_VERSION)

        self._set_taa_to_state(digest, seq_no, txn_time, text, version, ratification_ts, retirement_ts)

    def _decode_state_value(self, encoded):
        if encoded:
            value, last_seq_no, last_update_time = decode_state_value(encoded,
                                                                      serializer=config_state_serializer)
            return value, last_seq_no, last_update_time
        return None, None, None

    def _validate_add_taa(self, request: Request, req_pp_time: int):
        if request.operation.get(TXN_AUTHOR_AGREEMENT_TEXT) is None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Cannot create transaction author agreement without a '{}' field."
                                       .format(TXN_AUTHOR_AGREEMENT_TEXT))

        if request.operation.get(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS) is not None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Cannot create transaction author agreement with a '{}' field."
                                       .format(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS))

        ratification_ts = request.operation.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS)
        if ratification_ts is None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Cannot create transaction author agreement without a '{}' field."
                                       .format(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS))

        if ratification_ts > req_pp_time:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Cannot create transaction author agreement with '{}' set in future."
                                       .format(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS))

    def _validate_update_taa(self, request, digest):
        ledger_taa = self.get_from_state(StaticTAAHelper.state_path_taa_digest(digest))[0]

        # check TAA text
        taa_text = ledger_taa.get(TXN_AUTHOR_AGREEMENT_TEXT)
        if request.operation.get(TXN_AUTHOR_AGREEMENT_TEXT, taa_text) != taa_text:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Changing a text of existing transaction author agreement is forbidden")

        # check TAA ratification timestamp
        taa_ratified = ledger_taa.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS)
        if request.operation.get(TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, taa_ratified) != taa_ratified:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Changing ratification date of existing "
                                       "transaction author agreement is forbidden")

        # TODO: Following code assumes that the only reason for updating TAA is changing its retirement date.
        #   If this is no longer the case this needs to be changed. Also this cries for adding separate transaction
        #   for TAA retirement

        # check if TAA enforcement is disabled
        last_taa_digest = StaticTAAHelper.get_latest_taa(self.state)
        if last_taa_digest is None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Retirement date cannot be changed when TAA enforcement is disabled.")

        # check if we are trying to modify latest TAA
        if last_taa_digest == digest:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "The latest transaction author agreement cannot be retired.")

    def _validate_ts(self, ts, identifier, req_id, field_name):
        if not ts:
            return
        try:
            datetime.utcfromtimestamp(ts)
        except ValueError:
            raise InvalidClientRequest(identifier, req_id,
                                       "{} = {} is out of range.".format(field_name, ts))

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID, TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS
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
        pass

    def dynamic_validation(self, request: Request):
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
            self._validate_add_taa(request)
        else:
            self._validate_update_taa(request, digest)

    def update_state(self, txn, prev_result, request, is_committed=False):
        self._validate_txn_type(txn)
        payload = get_payload_data(txn)
        text = payload.get(TXN_AUTHOR_AGREEMENT_TEXT)
        version = payload[TXN_AUTHOR_AGREEMENT_VERSION]
        retired = payload.get(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS)
        seq_no = get_seq_no(txn)
        txn_time = get_txn_time(txn)
        digest = StaticTAAHelper.get_taa_digest(self.state, version, isCommitted=False)
        if digest is None:
            digest = StaticTAAHelper.taa_digest(text, version)
        self._update_txn_author_agreement(digest, seq_no, txn_time, text, version, retired)

    def authorize(self, request):
        StaticTAAHelper.authorize(self.database_manager, request)

    def _decode_state_value(self, encoded):
        if encoded:
            value, last_seq_no, last_update_time = decode_state_value(encoded,
                                                                      serializer=config_state_serializer)
            return value, last_seq_no, last_update_time
        return None, None, None

    def _validate_add_taa(self, request):
        if request.operation.get(TXN_AUTHOR_AGREEMENT_TEXT) is None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Cannot create a transaction author agreement without a '{}' field."
                                       .format(TXN_AUTHOR_AGREEMENT_TEXT))

        if request.operation.get(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS) is not None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Cannot create a transaction author agreement with a '{}' field."
                                       .format(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS))

    def _validate_update_taa(self, request, digest):
        # check TAA text
        ledger_taa = self.get_from_state(StaticTAAHelper.state_path_taa_digest(digest))[0]
        taa_text = ledger_taa.get(TXN_AUTHOR_AGREEMENT_TEXT)
        if request.operation.get(TXN_AUTHOR_AGREEMENT_TEXT, taa_text) != taa_text:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Changing a text of existing transaction author agreement is forbidden")
        # check the latest TAA
        if TXN_AUTHOR_AGREEMENT_RETIREMENT_TS in request.operation:
            last_taa_digest = StaticTAAHelper.get_latest_taa(self.state)
            if last_taa_digest == digest:
                raise InvalidClientRequest(request.identifier, request.reqId,
                                           "The latest transaction author agreement cannot be retired.")
            if last_taa_digest is None:
                raise InvalidClientRequest(request.identifier, request.reqId,
                                           "Retirement date cannot be changed when TAA enforcement is disabled.")

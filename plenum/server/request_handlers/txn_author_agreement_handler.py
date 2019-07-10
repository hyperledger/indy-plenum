from _sha256 import sha256
from functools import lru_cache

from common.serializers.serialization import pool_state_serializer, config_state_serializer
from plenum.common.constants import POOL_LEDGER_ID, NODE, DATA, BLS_KEY, \
    BLS_KEY_PROOF, TARGET_NYM, DOMAIN_LEDGER_ID, NODE_IP, \
    NODE_PORT, CLIENT_IP, CLIENT_PORT, ALIAS, TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID, GET_TXN_AUTHOR_AGREEMENT_VERSION, \
    GET_TXN_AUTHOR_AGREEMENT_DIGEST, GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP, TXN_AUTHOR_AGREEMENT_VERSION, \
    TXN_AUTHOR_AGREEMENT_TEXT
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_from, get_seq_no, get_txn_time
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.utils import is_steward, encode_state_value, decode_state_value


class TxnAuthorAgreementHandler(WriteRequestHandler):

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
        if StaticTAAHelper.get_taa_digest(self.state, version, isCommitted=False) is not None:
            raise InvalidClientRequest(identifier, req_id,
                                       "Changing existing version of transaction author agreement is forbidden")

    def update_state(self, txn, prev_result, request, is_committed=False):
        self._validate_txn_type(txn)
        payload = get_payload_data(txn)
        text = payload[TXN_AUTHOR_AGREEMENT_TEXT]
        version = payload[TXN_AUTHOR_AGREEMENT_VERSION]
        seq_no = get_seq_no(txn)
        txn_time = get_txn_time(txn)
        self._update_txn_author_agreement(text, version, seq_no, txn_time)

    def _update_txn_author_agreement(self, text, version, seq_no, txn_time):
        digest = StaticTAAHelper.taa_digest(text, version)
        data = encode_state_value({
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version
        }, seq_no, txn_time, serializer=config_state_serializer)

        self.state.set(StaticTAAHelper.state_path_taa_digest(digest), data)
        self.state.set(StaticTAAHelper.state_path_taa_latest(), digest)
        self.state.set(StaticTAAHelper.state_path_taa_version(version), digest)

    def authorize(self, request):
        StaticTAAHelper.authorize(self.database_manager, request)

    def _decode_state_value(self, encoded):
        if encoded:
            value, last_seq_no, last_update_time = decode_state_value(encoded,
                                                                      serializer=config_state_serializer)
            return value, last_seq_no, last_update_time
        return None, None, None

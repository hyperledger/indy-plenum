from _sha256 import sha256
from typing import Optional, Dict

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML, GET_TXN_AUTHOR_AGREEMENT, \
    GET_TXN_AUTHOR_AGREEMENT_AML, TXN_TYPE, TXN_AUTHOR_AGREEMENT_VERSION, TXN_AUTHOR_AGREEMENT_TEXT, TRUSTEE, \
    AML, AML_VERSION, CONFIG_LEDGER_ID
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import (
    get_type, get_payload_data, get_seq_no, get_txn_time
)
from plenum.server.request_handlers.utils import (
    encode_state_value, decode_state_value
)
from plenum.server.domain_req_handler import DomainRequestHandler
from plenum.server.ledger_req_handler import LedgerRequestHandler
from storage.state_ts_store import StateTsDbStorage


class ConfigReqHandler(LedgerRequestHandler):
    write_types = {TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML}
    query_types = {GET_TXN_AUTHOR_AGREEMENT, GET_TXN_AUTHOR_AGREEMENT_AML}

    def __init__(self, ledger, state, domain_state, ts_store: Optional[StateTsDbStorage] = None):
        super().__init__(CONFIG_LEDGER_ID, ledger, state, ts_store)
        self._domain_state = domain_state

    def doStaticValidation(self, request: Request):
        identifier, req_id, operation = request.identifier, request.reqId, request.operation
        typ = operation.get(TXN_TYPE)

        if typ == TXN_AUTHOR_AGREEMENT_AML:
            if len(operation[AML]) == 0:
                raise InvalidClientRequest(identifier, req_id,
                                           "TAA AML request must contain at least one acceptance mechanism")

    def get_query_response(self, request):
        pass

    def validate(self, req: Request):
        identifier, req_id, operation = req.identifier, req.reqId, req.operation
        self.authorize(req)

        typ = operation.get(TXN_TYPE)
        if typ == TXN_AUTHOR_AGREEMENT:
            version = operation[TXN_AUTHOR_AGREEMENT_VERSION]
            if self.get_taa_digest(version, isCommitted=False) is not None:
                raise InvalidClientRequest(identifier, req_id,
                                           "Changing existing version of transaction author agreement is forbidden")
        elif typ == TXN_AUTHOR_AGREEMENT_AML:
            version = operation.get(AML_VERSION)
            if self.get_taa_aml_data(version, isCommitted=False) is not None:
                raise InvalidClientRequest(identifier, req_id,
                                           "Version of TAA AML must be unique and it cannot be modified")

    def authorize(self, req: Request):
        typ = req.operation.get(TXN_TYPE)

        if typ in [TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML] \
                and not self._is_trustee(req.identifier):
            raise UnauthorizedClientRequest(req.identifier, req.reqId,
                                            "Only trustee can update transaction author agreement and AML")

    def updateState(self, txns, isCommitted=False):
        for txn in txns:
            self.updateStateWithSingleTxn(txn, isCommitted=isCommitted)

    def updateStateWithSingleTxn(self, txn, isCommitted=False):
        typ = get_type(txn)
        payload = get_payload_data(txn)
        if typ == TXN_AUTHOR_AGREEMENT:
            payload = get_payload_data(txn)
            self.update_txn_author_agreement(
                payload[TXN_AUTHOR_AGREEMENT_TEXT],
                payload[TXN_AUTHOR_AGREEMENT_VERSION],
                get_seq_no(txn),
                get_txn_time(txn)
            )
        elif typ == TXN_AUTHOR_AGREEMENT_AML:
            self.update_txn_author_agreement_acceptance_mechanisms(payload)

    def update_txn_author_agreement(self, text, version, seq_no, txn_time):
        digest = self._taa_digest(text, version)
        data = encode_state_value({
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version
        }, seq_no, txn_time, serializer=config_state_serializer)

        self.state.set(self._state_path_taa_digest(digest), data)
        self.state.set(self._state_path_taa_latest(), digest)
        self.state.set(self._state_path_taa_version(version), digest)

    def update_txn_author_agreement_acceptance_mechanisms(self, payload):
        version = payload[AML_VERSION]
        self.state.set(self._state_path_taa_aml_latest(), config_state_serializer.serialize(payload))
        self.state.set(self._state_path_taa_aml_version(version), config_state_serializer.serialize(payload))

    def get_taa_digest(self, version: Optional[str] = None,
                       isCommitted: bool = True) -> Optional[str]:
        path = self._state_path_taa_latest() if version is None \
            else self._state_path_taa_version(version)
        res = self.state.get(path, isCommitted=isCommitted)
        if res is not None:
            return res.decode()

    def get_taa_data(self, digest: Optional[str] = None,
                     version: Optional[str] = None,
                     isCommitted: bool = True) -> Optional[Dict]:
        if digest is None:
            digest = self.get_taa_digest(version=version, isCommitted=isCommitted)
            if digest is None:
                return None
        data = self.state.get(
            self._state_path_taa_digest(digest),
            isCommitted=isCommitted
        )
        if data is None:
            return None
        return decode_state_value(data, serializer=config_state_serializer)

    def get_taa_aml_data(self, version: Optional[str] = None, isCommitted: bool = True):
        path = self._state_path_taa_aml_latest() if version is None \
            else self._state_path_taa_aml_version(version)
        return self.state.get(path, isCommitted=isCommitted)

    @staticmethod
    def _state_path_taa_latest():
        return b"taa:v:latest"

    @staticmethod
    def _state_path_taa_version(version: str):
        return "taa:v:{version}".format(version=version).encode()

    @staticmethod
    def _state_path_taa_digest(digest: str):
        return "taa:d:{digest}".format(digest=digest).encode()

    @staticmethod
    def _taa_digest(text: str, version: str) -> str:
        return sha256('{}{}'.format(version, text).encode()).hexdigest()

    @staticmethod
    def _state_path_taa_aml_latest():
        return b"taa:aml:latest"

    @staticmethod
    def _state_path_taa_aml_version(version: str):
        return "taa:aml:v:{version}".format(version=version).encode()

    def _is_trustee(self, nym: str):
        return bool(DomainRequestHandler.get_role(self._domain_state, nym,
                                                  TRUSTEE, isCommitted=False))

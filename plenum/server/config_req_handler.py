from _sha256 import sha256
from typing import Optional, Callable, Dict, Tuple

from common.serializers.serialization import config_state_serializer, state_roots_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML, GET_TXN_AUTHOR_AGREEMENT, \
    GET_TXN_AUTHOR_AGREEMENT_AML, TXN_TYPE, TXN_AUTHOR_AGREEMENT_VERSION, TXN_AUTHOR_AGREEMENT_TEXT, TRUSTEE, \
    TXN_TIME, CONFIG_LEDGER_ID, GET_TXN_AUTHOR_AGREEMENT_DIGEST, GET_TXN_AUTHOR_AGREEMENT_VERSION, \
    GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP, AML, AML_VERSION, GET_TXN_AUTHOR_AGREEMENT_AML_VERSION, \
    GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP

from plenum.common.types import f
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_type, get_payload_data, get_seq_no, get_txn_time
from plenum.server.request_handlers.utils import encode_state_value, decode_state_value
from plenum.server.domain_req_handler import DomainRequestHandler
from plenum.server.ledger_req_handler import LedgerRequestHandler
from storage.state_ts_store import StateTsDbStorage

MARKER_TAA = "2"
MARKER_TAA_AML = "3"


class ConfigReqHandler(LedgerRequestHandler):
    write_types = {TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML}
    query_types = {GET_TXN_AUTHOR_AGREEMENT, GET_TXN_AUTHOR_AGREEMENT_AML}

    def __init__(self, ledger, state, domain_state, bls_store, ts_store: Optional[StateTsDbStorage] = None):
        super().__init__(CONFIG_LEDGER_ID, ledger, state, ts_store)
        self._domain_state = domain_state
        # TODO: Move up to LedgerRequestHandler?
        self._bls_store = bls_store
        self._query_handlers = {}  # type: Dict[str, Callable]
        self._add_query_handler(GET_TXN_AUTHOR_AGREEMENT, self.handle_get_txn_author_agreement)
        self._add_query_handler(GET_TXN_AUTHOR_AGREEMENT_AML, self.handle_get_txn_author_agreement_aml)

    def doStaticValidation(self, request: Request):
        identifier, req_id, operation = request.identifier, request.reqId, request.operation
        typ = operation.get(TXN_TYPE)

        if typ == GET_TXN_AUTHOR_AGREEMENT:
            parameters = [GET_TXN_AUTHOR_AGREEMENT_VERSION,
                          GET_TXN_AUTHOR_AGREEMENT_DIGEST,
                          GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP]
            num_params = sum(1 for p in parameters if p in operation)
            if num_params > 1:
                raise InvalidClientRequest(identifier, req_id,
                                           "GET_TXN_AUTHOR_AGREEMENT request can have at most one of "
                                           "the following parameters: version, digest, timestamp")

        elif typ == TXN_AUTHOR_AGREEMENT_AML:
            if len(operation[AML]) == 0:
                raise InvalidClientRequest(identifier, req_id,
                                           "TXN_AUTHOR_AGREEMENT_AML request "
                                           "must contain at least one acceptance mechanism")
        elif typ == GET_TXN_AUTHOR_AGREEMENT_AML:
            if GET_TXN_AUTHOR_AGREEMENT_AML_VERSION in operation \
                    and GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP in operation:
                raise InvalidClientRequest(identifier, req_id,
                                           "\"version\" and \"timestamp\" cannot be used in "
                                           "GET_TXN_AUTHOR_AGREEMENT_AML request together")

    def get_query_response(self, request: Request):
        return self._query_handlers[request.operation.get(TXN_TYPE)](request)

    def validate(self, req: Request):
        identifier, req_id, operation = req.identifier, req.reqId, req.operation
        self.authorize(req)

        typ = operation.get(TXN_TYPE)
        if typ == TXN_AUTHOR_AGREEMENT:
            if self.state.get(self._state_path_taa_aml_latest()) is None:
                raise InvalidClientRequest(identifier, req_id,
                                           "TAA txn is forbidden until TAA AML is set. Send TAA AML first.")
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
        seq_no = get_seq_no(txn)
        txn_time = get_txn_time(txn)
        if typ == TXN_AUTHOR_AGREEMENT:
            self.update_txn_author_agreement(
                payload[TXN_AUTHOR_AGREEMENT_TEXT],
                payload[TXN_AUTHOR_AGREEMENT_VERSION],
                seq_no,
                txn_time
            )
        elif typ == TXN_AUTHOR_AGREEMENT_AML:
            self.update_txn_author_agreement_acceptance_mechanisms(payload,
                                                                   seq_no,
                                                                   txn_time)

    def update_txn_author_agreement(self, text, version, seq_no, txn_time):
        digest = self._taa_digest(text, version)
        data = encode_state_value({
            TXN_AUTHOR_AGREEMENT_TEXT: text,
            TXN_AUTHOR_AGREEMENT_VERSION: version
        }, seq_no, txn_time, serializer=config_state_serializer)

        self.state.set(self._state_path_taa_digest(digest), data)
        self.state.set(self._state_path_taa_latest(), digest)
        self.state.set(self._state_path_taa_version(version), digest)

    def update_txn_author_agreement_acceptance_mechanisms(self, payload, seq_no, txn_time):
        serialized_data = encode_state_value(payload, seq_no, txn_time, serializer=config_state_serializer)
        version = payload[AML_VERSION]
        self.state.set(self._state_path_taa_aml_latest(), serialized_data)
        self.state.set(self._state_path_taa_aml_version(version), serialized_data)

    def get_taa_digest(self, version: Optional[str] = None,
                       isCommitted: bool = True) -> Optional[str]:
        path = self._state_path_taa_latest() if version is None \
            else self._state_path_taa_version(version)
        res = self.state.get(path, isCommitted=isCommitted)
        if res is not None:
            return res.decode()

    # TODO return object as result instead
    def get_taa_data(self, digest: Optional[str] = None,
                     version: Optional[str] = None,
                     isCommitted: bool = True) -> Optional[Tuple[Dict, str]]:
        data = None
        if digest is None:
            digest = self.get_taa_digest(version=version, isCommitted=isCommitted)
        if digest is not None:
            data = self.state.get(
                self._state_path_taa_digest(digest),
                isCommitted=isCommitted
            )
        if data is not None:
            data = decode_state_value(
                data, serializer=config_state_serializer)
        return None if data is None else (data, digest)

    def get_taa_aml_data(self, version: Optional[str] = None, isCommitted: bool = True):
        path = self._state_path_taa_aml_latest() if version is None \
            else self._state_path_taa_aml_version(version)
        payload = self.state.get(path, isCommitted=isCommitted)
        if payload is None:
            return None
        return config_state_serializer.deserialize(payload)

    @staticmethod
    def _state_path_taa_latest() -> bytes:
        return "{marker}:latest". \
            format(marker=MARKER_TAA).encode()

    @staticmethod
    def _state_path_taa_version(version: str) -> bytes:
        return "{marker}:v:{version}". \
            format(marker=MARKER_TAA, version=version).encode()

    @staticmethod
    def _state_path_taa_digest(digest: str) -> bytes:
        return "{marker}:d:{digest}". \
            format(marker=MARKER_TAA, digest=digest).encode()

    @staticmethod
    def _taa_digest(text: str, version: str) -> str:
        return sha256('{}{}'.format(version, text).encode()).hexdigest()

    @staticmethod
    def _state_path_taa_aml_latest():
        return "{marker}:latest".format(marker=MARKER_TAA_AML).encode()

    @staticmethod
    def _state_path_taa_aml_version(version: str):
        return "{marker}:v:{version}".format(marker=MARKER_TAA_AML, version=version).encode()

    def _is_trustee(self, nym: str):
        return bool(DomainRequestHandler.get_role(self._domain_state, nym,
                                                  TRUSTEE, isCommitted=False))

    def _add_query_handler(self, txn_type, handler: Callable):
        if txn_type in self._query_handlers:
            raise ValueError('There is already a query handler registered '
                             'for {}'.format(txn_type))
        self._query_handlers[txn_type] = handler

    def get_value_from_state(self, path, head_hash=None, with_proof=False, multi_sig=None):
        '''
        Get a value (and proof optionally)for the given path in state trie.
        Does not return the proof is there is no aggregate signature for it.
        :param path: the path generate a state proof for
        :param head_hash: the root to create the proof against
        :param get_value: whether to return the value
        :return: a state proof or None
        '''
        if not multi_sig and with_proof:
            root_hash = head_hash if head_hash else self.state.committedHeadHash
            encoded_root_hash = state_roots_serializer.serialize(bytes(root_hash))

            multi_sig = self._bls_store.get(encoded_root_hash)
        return super().get_value_from_state(path, head_hash, with_proof, multi_sig)

    @staticmethod
    def make_config_result(request, data, last_seq_no=None, update_time=None, proof=None):
        result = LedgerRequestHandler.make_result(request, data, proof=proof)
        result[f.SEQ_NO.nm] = last_seq_no
        result[TXN_TIME] = update_time
        return result

    def handle_get_txn_author_agreement(self, request: Request):
        version = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_VERSION)
        digest = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_DIGEST)
        timestamp = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP)

        if version is not None:
            path = self._state_path_taa_version(version)
            digest, proof = self.get_value_from_state(path, with_proof=True)
            return self._return_txn_author_agreement(request, proof, digest=digest)

        if digest is not None:
            path = self._state_path_taa_digest(digest)
            data, proof = self.get_value_from_state(path, with_proof=True)
            return self._return_txn_author_agreement(request, proof, data=data)

        if timestamp is not None:
            head_hash = self.ts_store.get_equal_or_prev(timestamp, CONFIG_LEDGER_ID)
            if head_hash is None:
                return self._return_txn_author_agreement(request, None)
            path = self._state_path_taa_latest()
            digest, proof = self.get_value_from_state(path, head_hash, with_proof=True)
            return self._return_txn_author_agreement(request, proof, head_hash=head_hash, digest=digest)

        path = self._state_path_taa_latest()
        digest, proof = self.get_value_from_state(path, with_proof=True)
        return self._return_txn_author_agreement(request, proof, digest=digest)

    def _return_txn_author_agreement(self, request, proof, head_hash=None, digest=None, data=None):
        if digest is not None:
            head_hash = head_hash if head_hash else self.state.committedHeadHash
            data = self.state.get_for_root_hash(head_hash, self._state_path_taa_digest(digest.decode()))

        if data is not None:
            value, last_seq_no, last_update_time = decode_state_value(data, serializer=config_state_serializer)
            return self.make_config_result(request, value, last_seq_no, last_update_time, proof)

        return self.make_config_result(request, None, proof=proof)

    def handle_get_txn_author_agreement_aml(self, request: Request):
        version = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_AML_VERSION)
        timestamp = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP)

        if version is not None:
            path = self._state_path_taa_aml_version(version)
            data, proof = self.get_value_from_state(path, with_proof=True)
            return self._return_txn_author_agreement_aml(request, proof, data=data)

        if timestamp is not None:
            head_hash = self.ts_store.get_equal_or_prev(timestamp, CONFIG_LEDGER_ID)
            if head_hash is None:
                return self._return_txn_author_agreement_aml(request, None)
            head_hash = head_hash if head_hash else self.state.committedHeadHash
            data, proof = self.get_value_from_state(self._state_path_taa_aml_latest(), head_hash, with_proof=True)
            return self._return_txn_author_agreement_aml(request, proof, data=data)

        path = self._state_path_taa_aml_latest()
        data, proof = self.get_value_from_state(path, with_proof=True)
        return self._return_txn_author_agreement_aml(request, proof, data=data)

    def _return_txn_author_agreement_aml(self, request, proof, data=None):
        if data is not None:
            value, last_seq_no, last_update_time = decode_state_value(data, serializer=config_state_serializer)
            return self.make_config_result(request, value, last_seq_no, last_update_time, proof)

        return self.make_config_result(request, None, proof=proof)

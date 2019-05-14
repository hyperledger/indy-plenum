import json
from _sha256 import sha256
from typing import Optional, Callable, Dict

from common.serializers.serialization import config_state_serializer, state_roots_serializer
from plenum.common.constants import TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML, GET_TXN_AUTHOR_AGREEMENT, \
    GET_TXN_AUTHOR_AGREEMENT_AML, TXN_TYPE, TXN_AUTHOR_AGREEMENT_VERSION, TXN_AUTHOR_AGREEMENT_TEXT, TRUSTEE, \
    CONFIG_LEDGER_ID, TXN_TIME
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_type, get_payload_data
from plenum.common.types import f
from plenum.server.domain_req_handler import DomainRequestHandler
from plenum.server.ledger_req_handler import LedgerRequestHandler
from storage.state_ts_store import StateTsDbStorage


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

    def doStaticValidation(self, request: Request):
        pass

    def get_query_response(self, request: Request):
        return self._query_handlers[request.operation.get(TXN_TYPE)](request)

    def validate(self, req: Request):
        self.authorize(req)

        operation = req.operation
        typ = operation.get(TXN_TYPE)
        if typ == TXN_AUTHOR_AGREEMENT:
            version = operation[TXN_AUTHOR_AGREEMENT_VERSION]
            if self.get_taa_digest(version, isCommitted=False) is not None:
                raise InvalidClientRequest(req.identifier, req.reqId,
                                           "Changing existing version of transaction author agreement is forbidden")

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
            self.update_txn_author_agreement(payload)

    def update_txn_author_agreement(self, payload):
        version = payload[TXN_AUTHOR_AGREEMENT_VERSION]
        text = payload[TXN_AUTHOR_AGREEMENT_TEXT]
        digest = self._taa_digest(version, text)

        self.state.set(self._state_path_taa_latest(), digest)
        self.state.set(self._state_path_taa_version(version), digest)
        self.state.set(self._state_path_taa_digest(digest), config_state_serializer.serialize(payload))

    def get_taa_digest(self, version: Optional[str] = None, isCommitted: bool = True):
        path = self._state_path_taa_latest() if version is None \
            else self._state_path_taa_version(version)
        return self.state.get(path, isCommitted=isCommitted)

    @staticmethod
    def _state_path_taa_latest():
        return b"taa:latest"

    @staticmethod
    def _state_path_taa_version(version: str):
        return "taa:v:{version}".format(version=version).encode()

    @staticmethod
    def _state_path_taa_digest(digest: str):
        return "taa:d:{digest}".format(digest=digest).encode()

    @staticmethod
    def _taa_digest(version: str, text: str) -> str:
        return sha256('{}{}'.format(version, text).encode()).hexdigest()

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
        path = self._state_path_taa_latest()
        digest, proof = self.get_value_from_state(path, with_proof=True)
        if digest is None:
            return self.make_config_result(request, None, proof=proof)

        data = self.state.get(self._state_path_taa_digest(digest.decode()))
        data = json.loads(data.decode())
        # TODO: Fill in last_seq_no and update_time
        return self.make_config_result(request, data, proof=proof)

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import CONFIG_LEDGER_ID, GET_TXN_AUTHOR_AGREEMENT_VERSION, \
    GET_TXN_AUTHOR_AGREEMENT_DIGEST, GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP, \
    GET_TXN_AUTHOR_AGREEMENT
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.utils import decode_state_value


class GetTxnAuthorAgreementHandler(ReadRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, GET_TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID)

    def static_validation(self, request: Request):
        operation, identifier, req_id = request.operation, request.identifier, request.reqId
        parameters = [GET_TXN_AUTHOR_AGREEMENT_VERSION,
                      GET_TXN_AUTHOR_AGREEMENT_DIGEST,
                      GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP]
        num_params = sum(1 for p in parameters if p in operation)
        if num_params > 1:
            raise InvalidClientRequest(identifier, req_id,
                                       "GET_TXN_AUTHOR_AGREEMENT request can have at most one of "
                                       "the following parameters: version, digest, timestamp")

    def get_result(self, request: Request):
        version = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_VERSION)
        digest = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_DIGEST)
        timestamp = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP)

        if version is not None:
            path = StaticTAAHelper.state_path_taa_version(version)
            digest, proof = self._get_value_from_state(path, with_proof=True)
            return self._return_txn_author_agreement(request, proof, digest=digest)

        if digest is not None:
            path = StaticTAAHelper.state_path_taa_digest(digest)
            data, proof = self._get_value_from_state(path, with_proof=True)
            return self._return_txn_author_agreement(request, proof, data=data)

        if timestamp is not None:
            head_hash = self.database_manager.ts_store.get_equal_or_prev(timestamp, CONFIG_LEDGER_ID)
            if head_hash is None:
                return self._return_txn_author_agreement(request, None)
            path = StaticTAAHelper.state_path_taa_latest()
            digest, proof = self._get_value_from_state(path, head_hash, with_proof=True)
            return self._return_txn_author_agreement(request, proof, head_hash=head_hash, digest=digest)

        path = StaticTAAHelper.state_path_taa_latest()
        digest, proof = self._get_value_from_state(path, with_proof=True)
        return self._return_txn_author_agreement(request, proof, digest=digest)

    def _return_txn_author_agreement(self, request, proof, head_hash=None, digest=None, data=None):
        if digest is not None:
            head_hash = head_hash if head_hash else self.state.committedHeadHash
            data = self.state.get_for_root_hash(head_hash, StaticTAAHelper.state_path_taa_digest(digest.decode()))

        if data is not None:
            value, last_seq_no, last_update_time = decode_state_value(data, serializer=config_state_serializer)
            return self.make_result(request, value, last_seq_no, last_update_time, proof)

        return self.make_result(request, None, proof=proof)

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import CONFIG_LEDGER_ID, \
    GET_TXN_AUTHOR_AGREEMENT_AML, GET_TXN_AUTHOR_AGREEMENT_AML_VERSION, \
    GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.utils import decode_state_value


class GetTxnAuthorAgreementAmlHandler(ReadRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, GET_TXN_AUTHOR_AGREEMENT_AML, CONFIG_LEDGER_ID)

    def static_validation(self, request: Request):
        operation, identifier, req_id = request.operation, request.identifier, request.reqId
        if GET_TXN_AUTHOR_AGREEMENT_AML_VERSION in operation \
                and GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP in operation:
            raise InvalidClientRequest(identifier, req_id,
                                       "\"version\" and \"timestamp\" cannot be used in "
                                       "GET_TXN_AUTHOR_AGREEMENT_AML request together")

    def get_result(self, request: Request):
        version = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_AML_VERSION)
        timestamp = request.operation.get(GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP)

        if version is not None:
            path = StaticTAAHelper.state_path_taa_aml_version(version)
            data, proof = self._get_value_from_state(path, with_proof=True)
            return self._return_txn_author_agreement_aml(request, proof, data=data)

        if timestamp is not None:
            head_hash = self.database_manager.ts_store.get_equal_or_prev(timestamp, CONFIG_LEDGER_ID)
            if head_hash is None:
                return self._return_txn_author_agreement_aml(request, None)
            head_hash = head_hash if head_hash else self.state.committedHeadHash
            data, proof = self._get_value_from_state(StaticTAAHelper.state_path_taa_aml_latest(), head_hash, with_proof=True)
            return self._return_txn_author_agreement_aml(request, proof, data=data)

        path = StaticTAAHelper.state_path_taa_aml_latest()
        data, proof = self._get_value_from_state(path, with_proof=True)
        return self._return_txn_author_agreement_aml(request, proof, data=data)

    def _return_txn_author_agreement_aml(self, request, proof, data=None):
        if data is not None:
            value, last_seq_no, last_update_time = decode_state_value(data, serializer=config_state_serializer)
            return self.make_result(request, value, last_seq_no, last_update_time, proof)

        return self.make_result(request, None, proof=proof)

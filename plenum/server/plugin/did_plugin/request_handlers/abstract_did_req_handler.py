from abc import ABCMeta
from typing import Optional

from common.serializers.json_serializer import JsonSerializer
from plenum.common.constants import TXN_TYPE, DATA
from plenum.common.exceptions import InvalidClientRequest, \
    UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.plugin.did_plugin.constants import DID_PLUGIN_LEDGER_ID


class AbstractDIDReqHandler(WriteRequestHandler, metaclass=ABCMeta):

    @property
    def did_dict(self):
        return self._did_dict

    def __init__(self, database_manager: DatabaseManager, txn_type, did_dict: dict):
        super().__init__(database_manager, txn_type, DID_PLUGIN_LEDGER_ID)
        self._did_dict = did_dict

    def static_validation(self, request: Request):
        """
        Ensure that the request payload has a 'data' field which is of type dict.
        """
        self._validate_request_type(request)
        identifier, req_id, operation = request.identifier, request.reqId, request.operation
        data = operation.get(DATA)
        if not isinstance(data, dict):
            msg = '{} attribute is missing or not in proper format'.format(DATA)
            raise InvalidClientRequest(identifier, req_id, msg)

    def additional_dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        """
        Ensure that the 'data' dict has a field 'id'
        """
        self._validate_request_type(request)
        operation = request.operation
        data = operation.get(DATA)
        if data['id'] not in self.did_dict:
            raise UnauthorizedClientRequest(request.identifier,
                                            request.reqId,
                                            'additional_dynamic_validation failed in AbstractDIDReqHandler')

    # def update_state(self, txn, prev_result, request, is_committed=False):
    #     data = get_payload_data(txn).get(DATA)
    #     for k, v in data.items():
    #         self.state.set(k.encode(), JsonSerializer.dumps(v))

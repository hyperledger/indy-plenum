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
from plenum.test.plugin.demo_plugin.constants import AUCTION_LEDGER_ID


class AbstractAuctionReqHandler(WriteRequestHandler, metaclass=ABCMeta):

    # This is for testing, not required to have
    STARTING_BALANCE = 1000

    @property
    def auctions(self):
        return self._auctions

    def __init__(self, database_manager: DatabaseManager, txn_type, auctions: dict):
        super().__init__(database_manager, txn_type, AUCTION_LEDGER_ID)
        self._auctions = auctions

    def static_validation(self, request: Request):
        self._validate_request_type(request)
        identifier, req_id, operation = request.identifier, request.reqId, request.operation
        data = operation.get(DATA)
        if not isinstance(data, dict):
            msg = '{} attribute is missing or not in proper format'.format(DATA)
            raise InvalidClientRequest(identifier, req_id, msg)

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        self._validate_request_type(request)
        operation = request.operation
        data = operation.get(DATA)
        if data['id'] not in self.auctions:
            raise UnauthorizedClientRequest(request.identifier,
                                            request.reqId,
                                            'unknown auction')

    def update_state(self, txn, prev_result, request, is_committed=False):
        data = get_payload_data(txn)
        for k, v in data.items():
            self.state.set(k.encode(), JsonSerializer.dumps(v))

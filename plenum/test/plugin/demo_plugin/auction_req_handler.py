from plenum.common.constants import TXN_TYPE, DATA
from plenum.common.exceptions import InvalidClientRequest, \
    UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn
from plenum.common.types import f
from plenum.persistence.util import txnsWithSeqNo
from plenum.server.req_handler import RequestHandler
from plenum.test.plugin.demo_plugin.constants import PLACE_BID, AUCTION_END, \
    AUCTION_START, GET_BAL, AMOUNT


class AuctionReqHandler(RequestHandler):
    write_types = {AUCTION_START, AUCTION_END, PLACE_BID}
    query_types = {GET_BAL, }

    # This is for testing, not required to have
    STARTING_BALANCE = 1000

    def __init__(self, ledger, state):
        super().__init__(ledger, state)
        self.auctions = {}
        self.query_handlers = {
            GET_BAL: self.handle_get_bal,
        }

    def get_query_response(self, request: Request):
        return self.query_handlers[request.operation[TXN_TYPE]](request)

    def handle_get_bal(self, request: Request):
        return {**request.operation, **{
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId,
        }}

    def doStaticValidation(self, request: Request):
        identifier, req_id, operation = request.identifier, request.reqId, request.operation
        data = operation.get(DATA)
        if not isinstance(data, dict):
            msg = '{} attribute is missing or not in proper format'.format(DATA)
            raise InvalidClientRequest(identifier, req_id, msg)

        if operation.get(TXN_TYPE) == PLACE_BID:
            amount = data.get(AMOUNT)
            if not (isinstance(amount, (int, float)) and amount > 0):
                msg = '{} must be present and should be a number ' \
                      'greater than 0'.format(amount)
                raise InvalidClientRequest(identifier, req_id, msg)

    def validate(self, req: Request):
        operation = req.operation
        data = operation.get(DATA)
        if operation.get(TXN_TYPE) != AUCTION_START:
            if data['id'] not in self.auctions:
                raise UnauthorizedClientRequest(req.identifier,
                                                req.reqId,
                                                'unknown auction')
        else:
            self.auctions[data['id']] = {}

    def apply(self, req: Request, cons_time: int):
        operation = req.operation
        data = operation.get(DATA)
        if operation.get(TXN_TYPE) == PLACE_BID:
            self.auctions[data['id']][req.identifier] = data[AMOUNT]

        txn = reqToTxn(req, cons_time)
        (start, end), _ = self.ledger.appendTxns(
            [self.transform_txn_for_ledger(txn)])
        self.updateState(txnsWithSeqNo(start, end, [txn]))
        return start, txn

    def updateState(self, txns, isCommitted=False):
        for txn in txns:
            self._updateStateWithSingleTxn(txn, isCommitted=isCommitted)

    def _updateStateWithSingleTxn(self, txn, isCommitted=False):
        # Not doing anything since it is a sample plugin maintaining in memory state
        pass

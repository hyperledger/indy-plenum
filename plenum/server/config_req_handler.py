from plenum.common.request import Request
from plenum.server.ledger_req_handler import LedgerRequestHandler


class ConfigReqHandler(LedgerRequestHandler):
    def __init__(self, ledger, state):
        super().__init__(ledger, state)

    def doStaticValidation(self, request: Request):
        pass

    def get_query_response(self, request):
        pass

    def validate(self, req: Request):
        pass

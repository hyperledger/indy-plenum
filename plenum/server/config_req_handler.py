from plenum.server.ledger_req_handler import LedgerRequestHandler


class ConfigReqHandler(LedgerRequestHandler):
    def __init__(self, ledger, state):
        super().__init__(ledger, state)

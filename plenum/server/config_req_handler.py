from plenum.server.req_handler import RequestHandler


class ConfigReqHandler(RequestHandler):
    def __init__(self, ledger, state):
        super().__init__(ledger, state)

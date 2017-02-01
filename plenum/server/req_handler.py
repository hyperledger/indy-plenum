from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.common.state import State


class ReqHandler:
    @staticmethod
    def processReq(req: Request, ledger: Ledger, state: State):
        pass

    @staticmethod
    def validateReq(req: Request, ledger: Ledger, state: State):
        pass

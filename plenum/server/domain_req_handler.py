import json

from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.ledger import Ledger
from plenum.common.log import getlogger
from plenum.common.request import Request
from plenum.common.state import State
from plenum.common.txn import TXN_TYPE, NYM, ROLE, STEWARD, TARGET_NYM, VERKEY
from plenum.common.txn_util import reqToTxn

logger = getlogger()


class DomainReqHandler:
    def __init__(self, ledger, state, reqProcessors):
        self.ledger = ledger
        self.state = state
        self.reqProcessors = reqProcessors

    def validateReq(self, req: Request, config):
        if req.operation.get(TXN_TYPE) == NYM:
            origin = req.identifier
            error = None
            if not self.isSteward(self.state,
                             origin, isCommitted=False):
                error = "Only Steward is allowed to do this transactions"
            if req.operation.get(ROLE) == STEWARD:
                if self.stewardThresholdExceeded(config):
                    error = "New stewards cannot be added by other stewards as " \
                           "there are already {} stewards in the system".\
                        format(config.stewardThreshold)
            if error:
                raise UnauthorizedClientRequest(req.identifier,
                                                req.reqId,
                                                error)

    def reqToTxn(self, req: Request):
        txn = reqToTxn(req)
        for processor in self.reqProcessors:
            res = processor.process(req)
            txn.update(res)

        return txn

    def applyReq(self, req: Request):
        operation = req.operation
        typ = operation.get(TXN_TYPE)
        txn = self.reqToTxn(req)
        nym = operation.get(TARGET_NYM)
        self.ledger.appendTxns([txn])
        if typ == NYM:
            self.updateNym(nym, {
                ROLE: operation.get(ROLE),
                VERKEY: operation.get(VERKEY)
            }, isCommitted=False)
            return True
        else:
            logger.debug('Cannot apply request of type {}'.format(typ))
            return False

    def countStewards(self) -> int:
        """Count the number of stewards added to the pool transaction store"""
        allTxns = self.ledger.getAllTxn().values()
        return sum(1 for txn in allTxns if (txn[TXN_TYPE] == NYM) and
                   (txn.get(ROLE) == STEWARD))

    def stewardThresholdExceeded(self, config) -> bool:
        """We allow at most `stewardThreshold` number of  stewards to be added
        by other stewards"""
        return self.countStewards() > config.stewardThreshold

    def updateNym(self, nym, data, isCommitted=True):
        existingData = self.getSteward(self.state, nym, isCommitted=isCommitted)
        existingData.update(data)
        self.state.set(nym.encode(), json.dumps(data).encode())

    @staticmethod
    def getSteward(state, nym, isCommitted: bool = True):
        key = nym.encode()
        data = state.get(key, isCommitted)
        return json.loads(data) if data else {}

    @staticmethod
    def isSteward(state, nym, isCommitted: bool = True):
        return DomainReqHandler.getSteward(state, nym, isCommitted).get(ROLE) == STEWARD

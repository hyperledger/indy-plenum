import json

from ledger.serializers.json_serializer import JsonSerializer
from ledger.util import F
from plenum.common.constants import TXN_TYPE, NYM, ROLE, STEWARD, TARGET_NYM, VERKEY
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn
from plenum.common.types import f
from plenum.persistence.util import txnsWithSeqNo
from plenum.server.req_handler import RequestHandler
from stp_core.common.log import getlogger

logger = getlogger()


class DomainRequestHandler(RequestHandler):
    stateSerializer = JsonSerializer()

    def __init__(self, ledger, state, reqProcessors):
        super().__init__(ledger, state)
        self.reqProcessors = reqProcessors

    def validate(self, req: Request, config=None):
        if req.operation.get(TXN_TYPE) == NYM:
            origin = req.identifier
            error = None
            if not self.isSteward(self.state,
                                  origin, isCommitted=False):
                error = "Only Steward is allowed to do these transactions"
            if req.operation.get(ROLE) == STEWARD:
                if self.stewardThresholdExceeded(config):
                    error = "New stewards cannot be added by other stewards " \
                            "as there are already {} stewards in the system".\
                            format(config.stewardThreshold)
            if error:
                raise UnauthorizedClientRequest(req.identifier,
                                                req.reqId,
                                                error)

    def _reqToTxn(self, req: Request, cons_time: int):
        txn = reqToTxn(req, cons_time)
        for processor in self.reqProcessors:
            res = processor.process(req)
            txn.update(res)

        return txn

    def apply(self, req: Request, cons_time: int):
        txn = self._reqToTxn(req, cons_time)
        (start, end), _ = self.ledger.appendTxns([self.transform_txn_for_ledger(txn)])
        self.updateState(txnsWithSeqNo(start, end, [txn]))
        return txn

    @staticmethod
    def transform_txn_for_ledger(txn):
        """
        Some transactions need to be updated before they can be stored in the
        ledger, eg. storing certain payload in another data store and only its
        hash in the ledger
        """
        return txn

    def updateState(self, txns, isCommitted=False):
        for txn in txns:
            self._updateStateWithSingleTxn(txn, isCommitted=isCommitted)

    def _updateStateWithSingleTxn(self, txn, isCommitted=False):
        typ = txn.get(TXN_TYPE)
        if typ == NYM:
            nym = txn.get(TARGET_NYM)
            self.updateNym(nym, txn, isCommitted=isCommitted)
        else:
            logger.debug('Cannot apply request of type {} to state'.format(typ))

    def countStewards(self) -> int:
        """
        Count the number of stewards added to the pool transaction store
        Note: This is inefficient, a production use case of this function
        should require an efficient storage mechanism
        """
        # THIS SHOULD NOT BE DONE FOR PRODUCTION
        return sum(1 for _, txn in self.ledger.getAllTxn() if
                   (txn[TXN_TYPE] == NYM) and (txn.get(ROLE) == STEWARD))

    def stewardThresholdExceeded(self, config) -> bool:
        """We allow at most `stewardThreshold` number of  stewards to be added
        by other stewards"""
        return self.countStewards() > config.stewardThreshold

    def updateNym(self, nym, txn, isCommitted=True):
        existingData = self.getNymDetails(self.state, nym,
                                          isCommitted=isCommitted)
        newData = {}
        if not existingData:
            # New nym being added to state, set the TrustAnchor
            newData[f.IDENTIFIER.nm] = txn[f.IDENTIFIER.nm]
            # New nym being added to state, set the role and verkey to None, this makes
            # the state data always have a value for `role` and `verkey` since we allow
            # clients to omit specifying `role` and `verkey` in the request consider a
            # default value of None
            newData[ROLE] = None
            newData[VERKEY] = None

        if ROLE in txn:
            newData[ROLE] = txn[ROLE]
        if VERKEY in txn:
            newData[VERKEY] = txn[VERKEY]
        newData[F.seqNo.name] = txn.get(F.seqNo.name)
        existingData.update(newData)
        key = nym.encode()
        val = self.stateSerializer.serialize(existingData)
        self.state.set(key, val)
        return existingData

    def hasNym(self, nym, isCommitted: bool = True):
        key = nym.encode()
        data = self.state.get(key, isCommitted)
        return bool(data)

    @staticmethod
    def getSteward(state, nym, isCommitted: bool = True):
        nymData = DomainRequestHandler.getNymDetails(state, nym, isCommitted)
        if not nymData:
            return {}
        else:
            if nymData.get(ROLE) == STEWARD:
                return nymData
            else:
                return {}

    @staticmethod
    def isSteward(state, nym, isCommitted: bool = True):
        return bool(DomainRequestHandler.getSteward(state,
                                                    nym,
                                                    isCommitted))

    @staticmethod
    def getNymDetails(state, nym, isCommitted: bool = True):
        key = nym.encode()
        data = state.get(key, isCommitted)
        return json.loads(data.decode()) if data else {}

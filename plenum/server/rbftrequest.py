from typing import Iterable
from collections import defaultdict
from enum import unique, IntEnum

from stp_core.common.log import getlogger
from plenum.common.request import Request
from plenum.server.quorums import Quorum
from plenum.server.stateful import Stateful, TransitionError, StatefulEvent
from plenum.server.tpcrequest import TPCReqEvent, TPCRequest


logger = getlogger()


@unique
class RBFTReqState(IntEnum):
    Propagation = 0     # propagating and waiting for consesus
    Finalized = 1       # have finalized version of request approved by consensus of nodes
    Forwarded = 2       # was forwarded to all protocol instances (replicas)
    Rejected = 3        # was rejected
    Committed = 4       # was committed
    Replyed = 5         # replyed to client about either commit or reject
    Executed = 6        # no more oprations are expected
    Detached = 7        # executed and no replicas operate with it
    # TODO RequestAck if needed
    # TODO RequestNAck if needed


class RBFTRequest(Stateful):
    """
    Client request with additional logic to hold RBFT related things
    """
    def __init__(self,
                 origRequest: Request,
                 nodeName: str,
                 clientName: str,
                 masterInstId: int=0):

        self.origRequest = origRequest
        self._clientName = clientName
        self._nodeName = nodeName
        self.masterInstId = masterInstId

        self.propagates = {}

        # TODO use only one from finalize/finalise
        self.finalised = None
        # self.forwarded = False

        self.tpcRequests = {}

        Stateful.__init__(
            self,
            initialState=RBFTReqState.Propagation,
            transitions={
                RBFTReqState.Finalized: RBFTReqState.Propagation,
                RBFTReqState.Forwarded: RBFTReqState.Finalized,
                RBFTReqState.Rejected: self._isRejectable,
                RBFTReqState.Committed: self._isCommittable,
                RBFTReqState.Replyed:
                    (RBFTReqState.Committed, RBFTReqState.Rejected),
                RBFTReqState.Executed: (RBFTReqState.Replyed,),
                RBFTReqState.Detached: self._isDetachable,
            }
        )

    def __repr__(self):
        return ("{} {}, origRequest: {}, clientName: {}, masterInstId: {}, "
                "tpcRequests: {}".format(
                    self.nodeName,
                    Stateful.__repr__(self),
                    repr(self.origRequest),
                    self.clientName,
                    self.masterInstId,
                    repr(self.tpcRequests)))

    def _isRejectable(self):
        return (self.state() == RBFTReqState.Forwarded and
                self.tpcRequests[self.masterInstId].wasState(TPCReqState.Rejected))

    def _isCommittable(self):
        return (self.state() == RBFTReqState.Forwarded and
                self.tpcRequests[self.masterInstId].wasState(TPCReqState.Ordered))

    def _isDetachable(self):
        return (
            self.state() == RBFTReqState.Executed and
            not len([tpcReq for tpcReq in self.tpcRequests.values()
                    if tpcReq.state() != TPCReqState.Cleaned])
        )

    def _finalize(self, sender: str):
        # TODO why we did a kind of deep copy here in the past
        # (possibly because of possible duplicate request from the same sender
        # which overwrote the one before - doesn't happen for now)
        self.finalised = self.propagates[sender]
        self.setState(RBFTReqState.Finalized)

    def _sendersForRequestWithQuorum(self, quorum: Quorum) -> set:
        digests = defaultdict(set)
        # this is workaround because we are getting a propagate from somebody with
        # non-str (byte) name
        for sender, req in filter(lambda x: isinstance(
                x[0], str), self.propagates.items()):
            digests[req.digest].add(sender)
            if quorum.is_reached(len(digests[req.digest])):
                return digests[req.digest]
        return None  # return None explicitly

    @property
    def key(self):
        return self.request.key

    @property
    def request(self):
        return self.origRequest

    @property
    def clientName(self):
        return self._clientName

    @property
    def nodeName(self):
        return self._nodeName

    @property
    def executed(self):
        return self.wasState(RBFTReqState.Executed)

    @property
    def forwarded(self):
        return self.wasState(RBFTReqState.Forwarded)

    def hasPropagate(self, sender: str) -> bool:
        """
        Check whether the request specified has already been propagated.
        """
        return sender in self.propagates

    def votes(self) -> int:
        """
        Get the number of propagates
        """
        return len(self.propagates)

    # EVENTS

    def onPropagate(self, request: Request, sender: str, quorum: Quorum):
        """
        Add the specified request to the list of received PROPAGATEs.

        Try to finalize the request if the required conditions are met.

        Determines whether to finalize client REQUESTs to replicas, based on the
        following logic:

        - If exactly quorum of PROPAGATE requests are received, then finalize.
        - If less than quorum of requests then probably there's no consensus on the
            REQUEST, don't finalize.
        - If more than quorum then already finalized, so don't finalize

        Even if the node hasn't received the client REQUEST itself, if it has
        received enough number of PROPAGATE messages for the same, the REQUEST
        can be finalized.

        :param request: the REQUEST to add
        :param sender: the name of the node sending the msg
        :param quorum: quorum for PROPAGATES
        """
        assert request.key == self.request.key

        self.propagates[sender] = request

        reason = None

        if self.finalised is None:
            # If not enough Propogates, don't bother comparing
            if not quorum.is_reached(self.votes()):
                reason = 'not enough propagates'
            else:
                senders = self._sendersForRequestWithQuorum(quorum)

                if senders:
                    logger.debug("{} finalizing request".format(self))
                    # use arbitrary request as they should be the same
                    self._finalize(senders.pop())
                else:
                    reason = 'not enough the same propagates'

        if reason:
            logger.debug("{} not finalizing since {}".format(
                self, reason))

    def onForwarded(self, instIds: Iterable[int]):
        """
        It marks request as forwarded to replicas.
        """
        # TODO curretnly delayed forwarding (e.g. to newly created replica)
        # is not supported but it seems this is the case we should worry about
        assert self.masterInstId in instIds
        for instId in set(instIds):
            self.tpcRequests[instId] = TPCRequest(self, instId)
        self.setState(RBFTReqState.Forwarded)

    def onCommitted(self):
        self.setState(RBFTReqState.Committed)

    def onReplyed(self):
        self.setState(RBFTReqState.Replyed)

    def onExecuted(self):
        self.setState(RBFTReqState.Executed)
        self.setState(RBFTReqState.Detached, expectTrError=True)

    # a group of events as wrappers for managed TPCRequests
    def onTPCPp(self, instId: int, tpcKey: Tuple[int, int], valid: bool):
        self.tpcRequests[instId].onPP(tpcKey, valid)
        if not valid and instId == self.masterInstId:
            self.setState(RBFTReqState.Rejected)

    def onTPCOrder(self, instId: int):
        self.tpcRequests[instId].onOrder()

    def onTPCClean(self, instId: int):
        self.tpcRequests[instId].onClean()
        self.setState(RBFTReqState.Detached, expectTrError=True)

    def tryTPCState(self, state, instId: int):
        if instId in self.tpcRequests:
            self.tpcRequests[instId].tryState(state)
        else:
            # TODO imporve to make more helpful and understandable
            raise TransitionError(
                "No TPCRequest for instId {} found".format(),
                stateful=self,
                state=state
            )

    # --- EVENTS

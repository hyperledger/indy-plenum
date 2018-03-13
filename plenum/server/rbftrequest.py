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
    Executed = 6        # no more oprations are expected TODO what about detach or reset
    Detached = 7        # executed and no replicas operate with it
    # TODO RequestAck if needed
    # TODO RequestNAck if needed


class RBFTReqEvent(StatefulEvent):
    """ Base class for event expected by RBFTRequest """
    pass



class RBFTReqPropagate(RBFTReqEvent):
    def __init__(self, request: Request, sender: str, quorum: Quorum):
        self.request = request
        self.sender = sender
        self.quorum = quorum


class RBFTReqForward(RBFTReqEvent):
    """
    It marks request as forwarded to replicas.
    """
    def __init__(self, instIds: Iterable[int]):
        self.instIds = instIds


class RBFTReqReply(RBFTReqEvent):
    pass


class RBFTReqExecute(RBFTReqEvent):
    pass


# wrapper for managed TPCRequests events
class RBFTReqTPCEvent(RBFTReqEvent):
    def __init__(self, instId: int, tpc_event: TPCReqEvent):
        self.instId = instId
        self.tpc_event = tpc_event


class RBFTRequest(Stateful):
    """
    Client request with additional logic to hold RBFT related things
    """

    # events
    Propagate = RBFTReqPropagate
    Forward = RBFTReqForward
    Reply = RBFTReqReply
    Execute = RBFTReqExecute
    TPCEvent = RBFTReqTPCEvent

    def __init__(self,
                 origRequest: Request,
                 nodeName: str,
                 clientName: str,
                 master_inst_id: int=0):

        self.origRequest = origRequest
        self._clientName = clientName
        self._nodeName = nodeName
        self.master_inst_id = master_inst_id

        self.propagates = {}

        # TODO use only one from finalize/finalise
        self.finalised = None
        # self.forwarded = False

        self.tpcRequests = {}

        Stateful.__init__(
            self,
            initialState=RBFTReqState.Propagation,
            # TODO Rejected, Committed, Replyed and Executed for now
            # are tightly coupled with master instance's state only
            transitions={
                RBFTReqState.Finalized: RBFTReqState.Propagation,
                RBFTReqState.Forwarded: self._isResettable,
                RBFTReqState.Rejected: self._isRejectable,
                RBFTReqState.Committed: self._isCommittable,
                RBFTReqState.Replyed:
                    (RBFTReqState.Committed, RBFTReqState.Rejected),
                RBFTReqState.Executed: RBFTReqState.Replyed,
                RBFTReqState.Detached: self._isDetachable,
            }
        )

    def __repr__(self):
        return ("{} {}, origRequest: {}, clientName: {}, master_inst_id: {}, "
                "tpcRequests: {}".format(
                    self.nodeName,
                    Stateful.__repr__(self),
                    repr(self.origRequest),
                    self.clientName,
                    self.master_inst_id,
                    repr(self.tpcRequests)))

    def _isResettable(self):
        # catch-up can cause that
        return (self.state() == RBFTReqState.Finalized or
                (self._master_tpc_request is not None and
                    self._master_tpc_request.isReset()))

    def _isRejectable(self):
        return (self.state() == RBFTReqState.Forwarded and
                self._master_tpc_request.isRejected())

    def _isCommittable(self):
        return (self.state() == RBFTReqState.Forwarded and
                self._master_tpc_request.isCommitted())

    def _isDetachable(self):
        return (
            self.state() == RBFTReqState.Executed and
            not len([tpcReq for tpcReq in self.tpcRequests.values()
                    if not tpcReq.isCleaned()])
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
    def _master_tpc_request(self):
        return self.tpcRequests.get(self.master_inst_id)

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


    # EVENTS processing
    def _propagate(self, request: Request, sender: str, quorum: Quorum):
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

    def _on(self, ev, dry=False):
        if type(ev) == RBFTReqPropagate:
            assert ev.request.key == self.request.key

            if dry:
                raise NotImplementedError(
                        "{!r}: dry mode for react()".format(self))

            self._propagate(ev.request, ev.sender, ev.quorum)

        elif type(ev) == RBFTReqForward:
            if dry:
                raise NotImplementedError(
                    "{!r}: dry mode for react()".format(self))

            # TODO curretnly delayed forwarding (e.g. to newly created replica)
            # is not supported but it seems this is the case we should worry about
            assert self.master_inst_id in ev.instIds
            for instId in set(ev.instIds):
                self.tpcRequests[instId] = TPCRequest(self, instId)
            self.setState(RBFTReqState.Forwarded)

        elif type(ev) == RBFTReqReply:
            self.setState(RBFTReqState.Replyed, dry)

        elif type(ev) == RBFTReqExecute:
            self.setState(RBFTReqState.Executed, dry)
            if self._isDetachable():
                self.setState(RBFTReqState.Detached, dry)

        elif type(ev) == RBFTReqTPCEvent:
            if ev.instId not in self.tpcRequests:
                # TODO improve to make more helpful and understandable
                raise RuntimeError(
                    "{!r} No TPCRequest for instId {} found"
                    .format(self, ev.instId),
                )

            self.tpcRequests[ev.instId].on(ev.tpc_event, dry)

            if not dry:
                if isinstance(ev.tpc_event, TPCRequest.Clean):
                    if self._isDetachable():
                        self.setState(RBFTReqState.Detached)
                elif ev.instId == self.master_inst_id:
                    # we don't expect any transition errors here
                    # because all further self transitions depends
                    # on master instance state (as of now)
                    if isinstance(ev.tpc_event, TPCRequest.Reject):
                        assert self._isRejectable()
                        self.setState(RBFTReqState.Rejected)
                    elif isinstance(ev.tpc_event, TPCRequest.Commit):
                        assert self._isCommittable()
                        self.setState(RBFTReqState.Committed)
                    elif isinstance(ev.tpc_event, TPCRequest.Reset):
                        assert self._isResettable()
                        self.setState(RBFTReqState.Forwarded)


        else:
            logger.warning("{!r} unexpected event type: {}".format(self, type(ev)))

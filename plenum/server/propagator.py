from collections import OrderedDict, defaultdict
from enum import unique, IntEnum
from typing import Tuple, Union, List, Iterable
from orderedset import OrderedSet

from stp_core.common.log import getlogger
from plenum.common.constants import PROPAGATE, THREE_PC_PREFIX
from plenum.common.messages.node_messages import Propagate
from plenum.common.request import Request, ReqKey
from plenum.common.types import f
from plenum.server.quorums import Quorum
from plenum.server.stateful import Stateful, TransitionError


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
    def __init__(self, origRequest: Request, clientName: str=None, masterInstId: int=0):

        self.origRequest = origRequest
        self._clientName = clientName
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
        return ("{}, origRequest: {}, clientName: {}, masterInstId: {}, "
                "tpcRequests: {}".format(
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

    @clientName.setter
    def clientName(self, value):
        self._clientName = value

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
    def onTPCRejected(self, instId: int):
        self.tpcRequests[instId].onRejected()
        if instId == self.masterInstId:
            self.setState(RBFTReqState.Rejected)

    def onTPCPp(self, instId: int):
        self.tpcRequests[instId].onPP()

    def onTPCOrdered(self, instId: int):
        self.tpcRequests[instId].onOrdered()

    def onTPCCleaned(self, instId: int):
        self.tpcRequests[instId].onCleaned()
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


@unique
class TPCReqState(IntEnum):
    Forwarded = 1   # was forwarded to replica, waiting for 3PC routine
    Rejected = 2    # was was rejected during 3PC batch creation
    In3PC = 3       # was added to (received in) some PrePrepare
    Ordered = 4     # was ordered
    Cleaned = 5     # was cleaned (no more referenced)


class TPCRequest(Stateful):
    """
    3PC request
    """
    def __init__(self, rbftRequest: RBFTRequest, instId: int):

        self.rbftRequest = rbftRequest
        self.instId = instId
        Stateful.__init__(
            self,
            initialState=TPCReqState.Forwarded,
            transitions={
                TPCReqState.Rejected: TPCReqState.Forwarded,
                TPCReqState.In3PC: TPCReqState.Forwarded,
                TPCReqState.Ordered: TPCReqState.In3PC,
                TPCReqState.Cleaned: (
                    TPCReqState.Forwarded,
                    TPCReqState.Rejected,
                    TPCReqState.In3PC,
                    TPCReqState.Ordered
                )
            }
        )

    def __repr__(self):
        return "{}, rbftRequest: {}, instId: {}".format(
            Stateful.__repr__(self), repr(self.key), self.instId)

    @property
    def key(self):
        return self.rbftRequest.key

    # EVENTS

    def onPP(self):
        self.setState(TPCReqState.In3PC)

    def onOrdered(self):
        self.setState(TPCReqState.Ordered)

    def onRejected(self):
        self.setState(TPCReqState.Rejected)

    def onCleaned(self):
        self.setState(TPCReqState.Cleaned)

    # --- EVENTS


class Requests(OrderedDict):
    """
    Storing client request object corresponding to each client and its
    request id. Key of the dictionary is a Tuple2 containing identifier,
    requestId. Used when Node gets an ordered request by a replica and
    needs to execute the request. Once the ordered request is executed
    by the node and returned to the transaction store, the key for that
    request is popped out
    """

    def add(self, req: Request, clientName: str=None):
        """
        Add the specified request to this request store.
        """
        if req.key not in self:
            self[req.key] = RBFTRequest(req, clientName=clientName)
        return self[req.key]

    def _tryRemove(self, rbftRequest: RBFTRequest):
        if rbftRequest.state() == RBFTReqState.Detached:
            self.pop(rbftRequest.request.key, None)

    def executed(self, reqKey: Tuple):
        """
        Works together with 'onForwarded' and 'clean' methods.

        It makes request to be removed if all replicas request was
        forwarded to freed it.
        """
        rbftRequest = self[reqKey]
        rbftRequest.onExecuted()
        self._tryRemove(rbftRequest)

    def clean(self, request_key, instId):
        """
        Works together with 'onForwarded' and 'executed' methods.

        It makes request to be removed if all replicas request was
        forwarded to freed it and if request executor marked it as executed.
        """
        rbftRequest = self.get(request_key)
        if not rbftRequest:
            return
        rbftRequest.onTPCCleaned(instId)
        self._tryRemove(rbftRequest)

    def is_finalised(self, reqKey: Tuple[str, int]) -> bool:
        return reqKey in self and self[reqKey].finalised

    def digest(self, reqKey: Tuple) -> str:
        if reqKey in self and self[reqKey].finalised:
            return self[reqKey].finalised.digest


class Propagator:
    MAX_REQUESTED_KEYS_TO_KEEP = 1000

    def __init__(self):
        self.requests = Requests()
        self.requested_propagates_for = OrderedSet()

    @staticmethod
    def createPropagate(
            request: Union[Request, dict], client_name) -> Propagate:
        """
        Create a new PROPAGATE for the given REQUEST.

        :param request: the client REQUEST
        :return: a new PROPAGATE msg
        """
        if not isinstance(request, (Request, dict)):
            logger.error("{} Request not formatted properly to create propagate"
                         .format(THREE_PC_PREFIX))
            return
        logger.trace("Creating PROPAGATE for REQUEST {}".format(request))
        request = request.as_dict if isinstance(request, Request) else \
            request
        if isinstance(client_name, bytes):
            client_name = client_name.decode()
        return Propagate(request, client_name)

    # noinspection PyUnresolvedReferences
    def propagate(self, request: Request, sender: str, clientName):
        """
        Broadcast a PROPAGATE to all other nodes

        :param request: the REQUEST to propagate
        :param sender: sender Node the request came from, None for client
        :param clientName: name of the original sender (client)
        """
        rbftRequest = self.requests.add(request, clientName)

        # TODO why sender wan't checked in propagates before and
        # ovewrite was allowed/expected in the past
        if not (sender is None or rbftRequest.hasPropagate(sender)):
            rbftRequest.onPropagate(request, sender, self.quorums.propagate)
            reason = None

            # try forwarding
            if rbftRequest.forwarded:
                reason = 'already forwarded'
            elif not rbftRequest.finalised:
                reason = 'not finalized'
            else:
                # If haven't got the client request(REQUEST) for the
                # corresponding propagate request(PROPAGATE) but have enough
                # propagate requests to move ahead
                self._forward(rbftRequest)

            if reason is not None:
                logger.debug("{} not forwarding request {} to its replicas "
                             "since {}".format(self, request.key, reason))

        if rbftRequest.hasPropagate(self.name):
            logger.trace("{} already propagated {}".format(self, request))
        else:
            propagate = self.createPropagate(request, rbftRequest.clientName)
            logger.info(
                "{} propagating request {} from client {}"
                .format(self, (request.identifier, request.reqId),
                        rbftRequest.clientName),
                extra={"cli": True, "tags": ["node-propagate"]}
            )
            self.send(propagate)
            self.propagate(request, self.name, rbftRequest.clientName)

    def request_propagates(self, req_keys):
        """
        Request PROPAGATEs for the given request keys. Since replicas can
        request PROPAGATEs independently of each other, check if it has
        been requested recently
        :param req_keys:
        :return:
        """
        i = 0
        for (idr, req_id) in req_keys:
            if (idr, req_id) not in self.requested_propagates_for:
                self.request_msg(PROPAGATE, {f.IDENTIFIER.nm: idr,
                                             f.REQ_ID.nm: req_id})
                self._add_to_recently_requested((idr, req_id))
                i += 1
            else:
                logger.debug('{} already requested PROPAGATE recently for {}'.
                             format(self, (idr, req_id)))
        return i

    # noinspection PyUnresolvedReferences
    def _forward(self, request: RBFTRequest):
        """
        Forward the specified client REQUEST to the other replicas on this node

        :param request: the REQUEST to propagate
        """
        numReplicas = self.replicas.num_replicas
        logger.debug("{} forwarding request {} to {} replicas"
                     .format(self, request.key, numReplicas))
        self.replicas.pass_message(ReqKey(*request.key))
        # TODO expect specific numeration scheme: from 0 up numReplicas
        request.onForwarded(tuple(range(numReplicas)))
        self.monitor.requestUnOrdered(*request.key)

    def _add_to_recently_requested(self, key):
        while len(
                self.requested_propagates_for) > self.MAX_REQUESTED_KEYS_TO_KEEP:
            self.requested_propagates_for.pop(last=False)
        self.requested_propagates_for.add(key)

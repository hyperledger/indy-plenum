from typing import Dict, Tuple, Union

from plenum.common.types import Propagate
from plenum.common.request import Request
from plenum.common.log import getlogger
from plenum.common.util import checkIfMoreThanFSameItems

logger = getlogger()


class ReqState:
    """
    Object to store the state of the request.
    """
    def __init__(self, request: Request):
        self.request = request
        self.forwarded = False
        self.propagates = {}
        self.finalised = None

    def isFinalised(self, f):
        if self.finalised is None:
            req = checkIfMoreThanFSameItems([v.__getstate__() for v in
                                             self.propagates.values()], f)
            if req:
                self.finalised = Request.fromState(req)
        return self.finalised


class Requests(Dict[Tuple[str, int], ReqState]):
    """
    Storing client request object corresponding to each client and its
    request id. Key of the dictionary is a Tuple2 containing identifier,
    requestId. Used when Node gets an ordered request by a replica and
    needs to execute the request. Once the ordered request is executed
    by the node and returned to the transaction store, the key for that
    request is popped out
    """
    def add(self, req: Request):
        """
        Add the specified request to this request store.
        """
        key = req.key
        if key not in self:
            self[key] = ReqState(req)
        return self[key]

    def forwarded(self, req: Request) -> bool:
        """
        Returns whether the request has been forwarded or not
        """
        return self[req.key].forwarded

    def flagAsForwarded(self, req: Request):
        """
        Set the given request's forwarded attribute to True
        """
        self[req.key].forwarded = True

    def addPropagate(self, req: Request, sender: str):
        """
        Add the specified request to the list of received
        PROPAGATEs.

        :param req: the REQUEST to add
        :param sender: the name of the node sending the msg
        """
        data = self.add(req)
        data.propagates[sender] = req

    def votes(self, req) -> int:
        """
        Get the number of propagates for a given reqId and identifier.
        """
        try:
            votes = len(self[(req.identifier, req.reqId)].propagates)
        except KeyError:
            votes = 0
        return votes

    def canForward(self, req: Request, requiredVotes: int) -> bool:
        """
        Check whether the request specified is eligible to be forwarded to the
        protocol instances.
        """
        state = self[req.key]
        return not state.forwarded and state.isFinalised(requiredVotes)

    def hasPropagated(self, req: Request, sender: str) -> bool:
        """
        Check whether the request specified has already been propagated.
        """
        return req.key in self and sender in self[req.key].propagates

    def isFinalised(self, reqKey: Tuple[str, int]) -> bool:
        return reqKey in self and self[reqKey].finalised

    def digest(self, reqKey: Tuple) -> str:
        if reqKey in self and self[reqKey].finalised:
            return self[reqKey].finalised.digest
        else:
            return None


class Propagator:
    def __init__(self):
        self.requests = Requests()

    # noinspection PyUnresolvedReferences
    def propagate(self, request: Request, clientName):
        """
        Broadcast a PROPAGATE to all other nodes

        :param request: the REQUEST to propagate
        """
        if self.requests.hasPropagated(request, self.name):
            logger.trace("{} already propagated {}".format(self, request))
        else:
            self.requests.addPropagate(request, self.name)
            # Only propagate if the node is participating in the consensus
            #  process
            # which happens when the node has completed the catchup process
            if self.isParticipating:
                propagate = self.createPropagate(request, clientName)
                logger.display("{} propagating {} request {} from client {}".
                               format(self, request.identifier, request.reqId,
                                      clientName),
                               extra={"cli": True, "tags": ["node-propagate"]})
                self.send(propagate)

    @staticmethod
    def createPropagate(request: Union[Request, dict], clientName) -> Propagate:
        """
        Create a new PROPAGATE for the given REQUEST.

        :param request: the client REQUEST
        :return: a new PROPAGATE msg
        """
        if not isinstance(request, (Request, dict)):
            logger.error("Request not formatted properly to create propagate")
            return
        logger.debug("Creating PROPAGATE for REQUEST {}".format(request))
        request = request.__getstate__() if isinstance(request, Request) else \
            request
        return Propagate(request, clientName)

    # noinspection PyUnresolvedReferences
    def canForward(self, request: Request) -> bool:
        """
        Determine whether to forward client REQUESTs to replicas, based on the
        following logic:

        - If exactly f+1 PROPAGATE requests are received, then forward.
        - If less than f+1 of requests then probably there's no consensus on the
            REQUEST, don't forward.
        - If more than f+1 then already forwarded to replicas, don't forward

        Even if the node hasn't received the client REQUEST itself, if it has
        received enough number of PROPAGATE messages for the same, the REQUEST
        can be forwarded.

        :param request: the client REQUEST
        """
        return self.requests.canForward(request, self.f + 1)

    # noinspection PyUnresolvedReferences
    def forward(self, request: Request):
        """
        Forward the specified client REQUEST to the other replicas on this node

        :param request: the REQUEST to propagate
        """
        key = request.key
        logger.debug("{} forwarding client request {} to its replicas".
                     format(self, key))
        for repQueue in self.msgsToReplicas:
            repQueue.append(self.requests[key].finalised.reqDigest)
        self.monitor.requestUnOrdered(*key)
        self.requests.flagAsForwarded(request)

    # noinspection PyUnresolvedReferences
    def recordAndPropagate(self, request: Request, clientName):
        """
        Record the request in the list of requests and propagate.

        :param request:
        :param clientName:
        """
        self.requests.add(request)
        # # Only propagate if the node is participating in the consensus process
        # # which happens when the node has completed the catchup process
        self.propagate(request, clientName)
        self.tryForwarding(request)

    def tryForwarding(self, request: Request):
        """
        Try to forward the request if the required conditions are met.
        See the method `canForward` for the conditions to check before
        forwarding a request.
        """
        if self.canForward(request):
            # If haven't got the client request(REQUEST) for the corresponding
            # propagate request(PROPAGATE) but have enough propagate requests
            # to move ahead
            self.forward(request)
        else:
            logger.trace("{} cannot yet forward request {} to its replicas".
                         format(self, request))

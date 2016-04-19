import logging
from typing import Dict, Tuple, Union

from plenum.common.types import Request, Propagate

logger = logging.getLogger(__name__)


class ReqState:
    def __init__(self, request: Request):
        self.request = request
        self.forwarded = False
        self.propagates = set()


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
        key = req.key
        if key not in self:
            self[key] = ReqState(req)
        return self[key]

    def forwarded(self, req: Request):
        return self[req.key].forwarded

    def flagAsForwarded(self, req: Request):
        self[req.key].forwarded = True

    def addPropagate(self, req: Request, sender: str):
        """
        Add the specified PROPAGATE to this node's list of received
        PROPAGATEs.

        :param req: the REQUEST to add
        :param sender: the name of the node sending the msg
        """
        data = self.add(req)
        data.propagates.add(sender)

    def votes(self, req):
        try:
            votes = len(self[(req.identifier, req.reqId)].propagates)
        except KeyError:
            votes = 0
        return votes

    def canForward(self, req: Request, requiredVotes: int):
        return self.votes(req) == requiredVotes and not self[req.key].forwarded

    def hasPropagated(self, req: Request, sender: str):
        return req.key in self and sender in self[req.key].propagates

    def canPrepare(self, req: Request, requiredVotes: int):
        if requiredVotes is None:
            return False
        return self.votes(req) >= requiredVotes


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
            propagate = self.createPropagate(request, clientName)
            logger.debug("{} propagating {} request {} from client {}".
                         format(self, request.identifier, request.reqId, clientName),
                         extra={"cli": True})
            self.send(propagate)

    @staticmethod
    def createPropagate(request: Union[Request, dict], clientName) -> Propagate:
        """
        Create a new PROPAGATE for the given REQUEST.

        :param request: the client REQUEST
        :return: a new PROPAGATE msg
        """
        logging.debug("Creating PROPAGATE for REQUEST {}".format(request))
        return Propagate(request.__getstate__(), clientName)

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
        logging.debug("{} forwarding client request {} to its replicas".
                      format(self.name, request.key))
        for repQueue in self.msgsToReplicas:
            repQueue.append(request.reqDigest)
        self.monitor.requestUnOrdered(*request.key)
        self.requests.flagAsForwarded(request)

    def recordAndPropagate(self, request: Request, clientName):
        self.requests.add(request)
        self.propagate(request, clientName)
        self.tryForwarding(request)

    def tryForwarding(self, request: Request):
        if self.canForward(request):
            # If haven't got the client request(REQUEST) for the corresponding
            # propagate request(PROPAGATE) but have enough propagate requests
            # to move ahead
            self.forward(request)
        else:
            logger.trace("{} cannot yet forward request {} to its replicas".
                         format(self, request))

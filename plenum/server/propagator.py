from collections import OrderedDict, Counter, defaultdict
from itertools import groupby

from typing import Dict, Tuple, Union, Optional

from plenum.common.messages.node_messages import Propagate
from plenum.common.request import Request, ReqKey
from stp_core.common.log import getlogger

logger = getlogger()


class ReqState:
    """
    Object to store the state of the request.
    """
    def __init__(self, request: Request):
        self.request = request
        self.forwarded = False
        # forwardedTo helps in finding to how many replicas has this request
        # been forwarded to, helps in garbage collection, see `gc` of `Replica`
        self.forwardedTo = 0
        self.propagates = {}
        self.finalised = None

    @property
    def most_propagated_request_with_senders(self):
        groups = defaultdict(set)
        # this is workaround because we are getting a propagate from somebody with
        # non-str (byte) name
        propagates = filter(lambda x: type(x[0]) == str, self.propagates.items())
        for key, value in sorted(propagates):
            groups[value].add(key)
        most_common_requests = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)
        return most_common_requests[0] if most_common_requests else (None, set())

    def set_finalised(self, req):
        # TODO: make it much explicitly and simpler
        # !side affect! if `req` is an instance of a child of `Request` class
        # here we construct the parent from child it is rather implicit that
        # `finalised` contains not the same type than `propagates` has
        self.finalised = Request.fromState(req.__getstate__())


class Requests(OrderedDict):
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

    def flagAsForwarded(self, req: Request, to: int):
        """
        Set the given request's forwarded attribute to True
        """
        self[req.key].forwarded = True
        self[req.key].forwardedTo = to

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

    def most_propagated_request_with_senders(self, req: Request):
        state = self[req.key]
        return state.most_propagated_request_with_senders

    def set_finalised(self, req: Request):
        state = self[req.key]
        state.set_finalised(req)

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
            propagate = self.createPropagate(request, clientName)
            logger.info("{} propagating {} request {} from client {}".
                           format(self, request.identifier, request.reqId,
                                  clientName),
                           extra={"cli": True, "tags": ["node-propagate"]})
            self.send(propagate)

    @staticmethod
    def createPropagate(request: Union[Request, dict], identifier) -> Propagate:
        """
        Create a new PROPAGATE for the given REQUEST.

        :param request: the client REQUEST
        :return: a new PROPAGATE msg
        """
        if not isinstance(request, (Request, dict)):
            logger.error("Request not formatted properly to create propagate")
            return
        logger.debug("Creating PROPAGATE for REQUEST {}".format(request))
        request = request.as_dict if isinstance(request, Request) else \
            request
        if isinstance(identifier, bytes):
            identifier = identifier.decode()
        return Propagate(request, identifier)

    # noinspection PyUnresolvedReferences
    def canForward(self, request: Request):
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

        if self.requests.forwarded(request):
            return 'already forwarded'

        req, senders = self.requests.most_propagated_request_with_senders(request)
        if self.name in senders:
            senders.remove(self.name)
        if req and self.quorums.propagate.is_reached(len(senders)):
            self.requests.set_finalised(req)
            return None
        else:
            return 'not finalised'

    # noinspection PyUnresolvedReferences
    def forward(self, request: Request):
        """
        Forward the specified client REQUEST to the other replicas on this node

        :param request: the REQUEST to propagate
        """
        key = request.key
        logger.debug('{} forwarding request {} to replicas'.format(self, key))
        for q in self.msgsToReplicas:
            q.append(ReqKey(*key))

        self.monitor.requestUnOrdered(*key)
        self.requests.flagAsForwarded(request, len(self.msgsToReplicas))

    # noinspection PyUnresolvedReferences
    def recordAndPropagate(self, request: Request, clientName):
        """
        Record the request in the list of requests and propagate.

        :param request:
        :param clientName:
        """
        self.requests.add(request)
        self.propagate(request, clientName)
        self.tryForwarding(request)

    def tryForwarding(self, request: Request):
        """
        Try to forward the request if the required conditions are met.
        See the method `canForward` for the conditions to check before
        forwarding a request.
        """
        cannot_reason_msg = self.canForward(request)
        if cannot_reason_msg is None:
            # If haven't got the client request(REQUEST) for the corresponding
            # propagate request(PROPAGATE) but have enough propagate requests
            # to move ahead
            self.forward(request)
        else:
            logger.debug("{} not forwarding request {} to its replicas "
                         "since {}".format(self, request, cannot_reason_msg))

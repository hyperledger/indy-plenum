from collections import OrderedDict
from typing import Tuple, Union
from orderedset import OrderedSet

from stp_core.common.log import getlogger
from plenum.common.constants import PROPAGATE, THREE_PC_PREFIX
from plenum.common.messages.node_messages import Propagate
from plenum.common.request import Request, ReqKey
from plenum.common.types import f
from plenum.server.rbftrequest import RBFTReqState, RBFTRequest

logger = getlogger()


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

    def process_write_request(self, request: Request, clientName: str):
        self.propagate(request, None, clientName)

    # noinspection PyUnresolvedReferences
    def propagate(self, request: Request, sender: str, clientName: str):
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

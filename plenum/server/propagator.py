from collections import OrderedDict
from typing import Tuple, Union
from orderedset import OrderedSet

from stp_core.common.log import getlogger
from plenum.common.constants import PROPAGATE, THREE_PC_PREFIX
from plenum.common.messages.node_messages import Propagate
from plenum.common.request import Request, ReqKey
from plenum.common.types import f
from plenum.server.tpcrequest import TPCRequest
from plenum.server.rbftrequest import RBFTRequest

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

    def add(self, req: Request, node_name: str,
            client_name: str, master_inst_id: int):
        """
        Add the specified request to this request store.
        """
        if req.key not in self:
            self[req.key] = RBFTRequest(req, node_name,
                                        client_name, master_inst_id)
        return self[req.key]

    def executed(self, req_key: Tuple):
        """
        Marks request as executed and tries to remove it
        """
        rbft_request = self[req_key]
        rbft_request.on_execute()
        if rbft_request.is_detached():
            self.pop(rbft_request.request.key, None)

    def clean(self, request_key, inst_id):
        """
        Marks request as cleaned for specified replica and tries to remove it
        """
        rbft_request = self.get(request_key)
        if not rbft_request:
            logger.warning(
                "Replica {} tries to clean unknown request {}"
                .format(inst_id, request_key))
            return

        rbft_request.on_tpcevent(inst_id, TPCRequest.Clean())
        if rbft_request.is_detached():
            self.pop(rbft_request.request.key, None)

    def is_finalised(self, req_key: Tuple[str, int]) -> bool:
        return req_key in self and self[req_key].finalised

    def digest(self, req_key: Tuple) -> str:
        if req_key in self and self[req_key].finalised:
            return self[req_key].finalised.digest


class Propagator:
    MAX_REQUESTED_KEYS_TO_KEEP = 1000

    def __init__(self):
        self.requests = Requests()
        self.requested_propagates_for = OrderedSet()

    @staticmethod
    def create_propagate(
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

    def process_write_request(self, request: Request, client_name: str):
        self.propagate(request, None, client_name)

    def propagate(self, request: Request, sender: str, client_name: str):
        """
        Broadcast a PROPAGATE to all other nodes

        :param request: the REQUEST to propagate
        :param sender: sender Node the request came from, None for client
        :param client_name: name of the original sender (client)
        """
        rbft_request = self.requests.add(
            request, self.name, client_name, self.instances.masterId)

        # TODO why sender wan't checked in propagates before and
        # ovewrite was allowed/expected in the past
        if not (sender is None or rbft_request.has_propagate(sender)):
            reason = None

            rbft_request.on_propagate(request, sender, self.quorums.propagate)

            # try forwarding
            if rbft_request.is_forwarded():
                reason = 'already forwarded'
            elif not rbft_request.finalised:
                reason = 'not finalized'
            else:
                # If haven't got the client request(REQUEST) for the
                # corresponding propagate request(PROPAGATE) but have enough
                # propagate requests to move ahead
                self._forward(rbft_request)

            if reason is not None:
                logger.debug("{} not forwarding request {} to its replicas "
                             "since {}".format(self, request.key, reason))

        if rbft_request.has_propagate(self.name):
            logger.trace("{} already propagated {}".format(self, request))
        else:
            propagate = self.create_propagate(request, rbft_request.client_name)
            logger.info(
                "{} propagating request {} from client {}"
                .format(self, (request.identifier, request.reqId),
                        rbft_request.client_name),
                extra={"cli": True, "tags": ["node-propagate"]}
            )
            self.send(propagate)
            self.propagate(request, self.name, rbft_request.client_name)

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

    def _forward(self, request: RBFTRequest):
        """
        Forward the specified client REQUEST to the other replicas on this node

        :param request: the REQUEST to propagate
        """
        num_replicas = self.replicas.num_replicas
        logger.debug("{} forwarding request {} to {} replicas"
                     .format(self, request.key, num_replicas))
        self.replicas.pass_message(ReqKey(*request.key))
        # TODO logic here relies on specific numeration scheme: from 0 up to 'num_replicas'
        request.on_forward(tuple(range(num_replicas)))
        self.monitor.requestUnOrdered(*request.key)

    def _add_to_recently_requested(self, key):
        while len(
                self.requested_propagates_for) > self.MAX_REQUESTED_KEYS_TO_KEEP:
            self.requested_propagates_for.pop(last=False)
        self.requested_propagates_for.add(key)

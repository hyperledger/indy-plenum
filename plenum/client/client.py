"""
A client in an RBFT system.
Client sends requests to each of the nodes,
and receives result of the request execution from nodes.
"""

import json
import logging
import time
from collections import deque
from typing import List, Union, Dict, Optional, Mapping, Tuple, Set

from plenum.common.motor import Motor
from plenum.common.request_types import Request, Reply, OP_FIELD_NAME, f
from plenum.common.startable import Status
from plenum.common.txn import REPLY
from plenum.common.util import getMaxFailures, getlogger
from raet.raeting import AutoMode

from plenum.client.signer import Signer, SimpleSigner
from plenum.common.stacked import NodeStacked, HA

logger = getlogger()


class Client(NodeStacked, Motor):

    def __init__(self,
                 clientId: str,
                 nodeReg: Dict[str, HA]=None,
                 ha: Union[HA, Tuple[str, int]]=None,
                 lastReqId: int = 0,
                 signer: Signer=None,
                 basedirpath: str=None):
        """
        Creates a new client.

        :param clientId: unique identifier for the client
        :param nodeReg: names and host addresses of all nodes in the pool
        :param lastReqId: Request Id of the last request sent by client
        :param stack: node stack or dictionary of node constructor kwargs
        :param signer: Helper for signer (defines sign method)
        """
        self.clientId = clientId
        self.lastReqId = lastReqId
        self._clientStack = None
        self.minimumNodes = getMaxFailures(len(nodeReg)) + 1

        cha = ha if isinstance(ha, HA) else HA(*ha)
        stackargs = dict(name=clientId,
                         ha=cha,
                         main=False,  # stops incoming vacuous joins
                         auto=AutoMode.always)
        if basedirpath:
            stackargs['basedirpath'] = basedirpath

        self.created = time.perf_counter()
        NodeStacked.__init__(self,
                             stackParams=stackargs,
                             nodeReg=nodeReg)
        logger.info("Client initialized with the following node registry:")
        lengths = [max(x) for x in zip(*[
            (len(name), len(host), len(str(port)))
            for name, (host, port) in nodeReg.items()])]
        fmt = "    {{:<{}}} listens at {{:<{}}} on port {{:>{}}}".format(
            *lengths)
        for name, (host, port) in nodeReg.items():
            logger.info(fmt.format(name, host, port))

        Motor.__init__(self)

        self.inBox = deque()

        self.signer = signer if signer else SimpleSigner(self.clientId)

        self.connectNicelyUntil = 0  # don't need to connect nicely as a client

    def start(self, loop):
        oldstatus = self.status
        super().start(loop)
        if oldstatus in Status.going():
            logger.info("{} is already {}, so start has no effect".
                        format(self, self.status.name))
        else:
            self.startNodestack()
            self.maintainConnections()

    async def prod(self, limit) -> int:
        """
        async function that returns the number of events

        :param limit: The number of messages to be processed
        :return: The number of events up to a prescribed `limit`
        """
        s = await self.nodestack.service(limit)
        await self.serviceLifecycle()
        self.flushOutBoxes()
        return s

    def createRequest(self, operation: Mapping) -> Request:
        """
        Client creates request which include requested operation and request Id

        :param operation: requested operation
        :return: New client request
        """
        request = Request(
                self.clientId, self.lastReqId + 1, operation)
        self.lastReqId += 1
        return request

    def submit(self, *operations: Mapping) -> List[Request]:
        """
        Sends an operation to the consensus pool

        :param operations: a sequence of operations
        :return: A list of client requests to be sent to the nodes in the system
        """
        requests = []
        for op in operations:
            request = self.createRequest(op)
            self.send(request)
            requests.append(request)
        return requests

    def sign(self, msg: Mapping) -> Mapping:
        """
        Signs the message if a signer is configured

        :param msg: Message to be signed
        :return: message
        """
        if f.SIG.nm not in msg or not msg[f.SIG.nm]:
            if self.signer:
                msg[f.SIG.nm] = self.signer.sign(msg)
            else:
                logger.warning("{} signer not configured so not signing {}".
                               format(self, msg))
        return msg

    def handleOneNodeMsg(self, wrappedMsg) -> None:
        """
        Handles single message from a node, and appends it to a queue

        :param wrappedMsg: Reply received by the client from the node
        """
        self.inBox.append(wrappedMsg)
        msg, frm = wrappedMsg
        logger.debug("Client {} got msg from node {}: {}".
                     format(self.clientId, frm, msg),
                     extra={"cli": True})

    def _statusChanged(self, old, new):
        # do nothing for now
        pass

    def onStopping(self, *args, **kwargs):
        if self.nodestack:
            self.nodestack.close()
            self.nodestack = None
        self.nextCheck = 0

    def getReply(self, reqId: int) -> Optional[Reply]:
        """
        Accepts reply message from node if the reply is matching

        :param reqId: Request Id
        :return: Reply message only when valid and matching
        (None, NOT_FOUND)
        (None, UNCONFIRMED) f+1 not reached
        (reply, CONFIRMED) f+1 reached

        """
        try:
            cons = self.hasConsensus(reqId)
        except KeyError:
            return None, "NOT_FOUND"
        if cons:
            return cons, "CONFIRMED"
        return None, "UNCONFIRMED"

    def getRepliesFromAllNodes(self, reqId: int):
        """
        Accepts a request ID and return a list of results from all the nodes
        for that request

        :param reqId: Request ID
        :return: list of request results from all nodes
        """
        return {frm: msg for msg, frm in self.inBox
                if msg[OP_FIELD_NAME] == REPLY and
                msg[f.REQ_ID.nm] == reqId}

    def hasConsensus(self, reqId: int) -> Optional[str]:
        """
        Accepts a request ID and returns True if consensus was reached
        for the request or else False

        :param reqId: Request ID
        :return: bool
        """
        replies = self.getRepliesFromAllNodes(reqId)
        if not replies:
            raise KeyError(reqId)  # NOT_FOUND
        # Check if at least f+1 replies are received or not.
        f = getMaxFailures(len(self.nodeReg))
        if f + 1 > len(replies):
            return False  # UNCONFIRMED
        else:
            onlyResults = {frm: reply['result'] for frm, reply in
                           replies.items()}
            resultsList = list(onlyResults.values())
            # if all the elements in the resultList are equal - consensus
            # is reached.
            if all(result == resultsList[0] for result in resultsList):
                return resultsList[0]  # CONFIRMED
            else:
                logging.error(
                    "Received a different result from at least one of the nodes..")
                # Now we need to know the counts of different results and so.
                jsonResults = [json.dumps(result, sort_keys=True) for result in
                               resultsList]
                # counts dictionary for calculating the count of different
                # results
                counts = {}
                for jresult in jsonResults:
                    counts[jresult] = counts.get(jresult, 0) + 1
                if counts[max(counts, key=counts.get)] > f + 1:
                    # CONFIRMED, as f + 1 matching results found
                    return json.loads(max(counts, key=counts.get))
                else:
                    # UNCONFIRMED, as f + 1 matching results are not found
                    return False

    def showReplyDetails(self, reqId: int):
        """
        Accepts a request ID and prints the reply details

        :param reqId: Request ID
        """
        replies = self.getRepliesFromAllNodes(reqId)
        replyInfo = "Node {} replied with result {}"
        if replies:
            for frm, reply in replies.items():
                print(replyInfo.format(frm, reply['result']))
        else:
            print("No replies received from Nodes!")

    def onConnsChanged(self, newConns: Set[str], lostConns: Set[str]):
        if self.isGoing():
            if len(self.conns) == len(self.nodeReg):
                self.status = Status.started
            elif len(self.conns) >= self.minimumNodes:
                self.status = Status.started_hungry


class ClientProvider:
    """
    Lazy client provider that takes a callback that returns a Client.
    It also shadows the client and when the client's any attribute is accessed the first time,
    it creates the client object using the callback.
    """

    def __init__(self, clientGenerator=None):
        """
        :param clientGenerator: Client generator
        """
        self.clientGenerator = clientGenerator
        self.client = None

    def __getattr__(self, attr):

        if attr not in ["clientGenerator", "client"]:
            if not self.client:
                self.client = self.clientGenerator()
            if hasattr(self.client, attr):
                logger.info(
                    "Client provider providing access to attribute {}".format(
                        attr))
                return getattr(self.client, attr)
            raise AttributeError(
                "Client has no attribute named {}".format(attr))

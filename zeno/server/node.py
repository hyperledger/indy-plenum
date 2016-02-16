"""
A node in an RBFT system.
Nodes communicate with each other via the RAET protocol. https://github.com/saltstack/raet
"""

import asyncio
import random
import time
from collections import deque
from functools import partial
from hashlib import sha256
from typing import Dict, Any, Mapping
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from raet.raeting import AutoMode

from zeno.common.exceptions import SuspiciousNode, SuspiciousClient, \
    MissingNodeOp, InvalidNodeOp, InvalidNodeMsg, InvalidClientMsgType, \
    InvalidClientOp, InvalidClientRequest, InvalidSignature
from zeno.common.motor import Motor
from zeno.common.request_types import Request, Propagate, \
    Reply, Nomination, OP_FIELD_NAME, TaggedTuples, Primary, \
    Reelection, PrePrepare, Prepare, Commit, \
    Ordered, RequestAck, InstanceChange, Batch, OPERATION, BlacklistMsg, f
from zeno.common.startable import Status
from zeno.common.transaction_store import TransactionStore
from zeno.common.util import getMaxFailures, MessageProcessor, getlogger
from zeno.server import primary_elector
from zeno.server.blacklister import Blacklister, SimpleBlacklister
from zeno.server.client_authn import ClientAuthNr, SimpleAuthNr
from zeno.server.has_action_queue import HasActionQueue
from zeno.server.models import InstanceChanges
from zeno.server.monitor import Monitor
from zeno.server.primary_decider import PrimaryDecider
from zeno.server.primary_elector import PrimaryElector
from zeno.server.propagator import Propagator
from zeno.server.router import Router

from zeno.common.stacked import HA, ClientStacked
from zeno.common.stacked import NodeStacked
from zeno.server import replica
from zeno.server.suspicion_codes import Suspicions

logger = getlogger()


class Node(HasActionQueue, NodeStacked, ClientStacked, Motor,
           Propagator, MessageProcessor):

    suspicions = {s.code: s.reason for s in Suspicions.getList()}

    def __init__(self,
                 name: str,
                 nodeRegistry: Dict[str, HA],
                 clientAuthNr: ClientAuthNr=None,
                 ha: HA=None,
                 cliname: str=None,
                 cliha: HA=None,
                 basedirpath: str=None,
                 primaryDecider: PrimaryDecider = None):
        """
        Create a new node.

        :param nodeRegistry: names and host addresses of all nodes in the pool
        :param clientAuthNr: client authenticator implementation to be used
        :param basedirpath: path to the base directory used by `nstack` and
            `cstack`
        :param primaryDecider: the mechanism to be used to decide the primary
        of a protocol instance
        """
        self.primaryDecider = primaryDecider
        me = nodeRegistry[name]

        self.allNodeNames = list(nodeRegistry.keys())
        if isinstance(me, NodeDetail):
            sha = me.ha
            scliname = me.cliname
            scliha = me.cliha
            nodeReg = {k: v.ha for k, v in nodeRegistry.items()}
        else:
            sha = me if isinstance(me, HA) else HA(*me)
            scliname = None
            scliha = None
            nodeReg = {k: HA(*v) for k, v in nodeRegistry.items()}
        if not ha:  # pull it from the registry
            ha = sha
        if not cliname:  # default to the name plus the suffix
            cliname = scliname if scliname else name + CLIENT_STACK_SUFFIX
        if not cliha:  # default to same ip, port + 1
            cliha = scliha if scliha else HA(ha[0], ha[1]+1)

        nstack = dict(name=name,
                      ha=ha,
                      main=True,
                      auto=AutoMode.never)

        cstack = dict(name=cliname,
                      ha=cliha,
                      main=True,
                      auto=AutoMode.always)

        if basedirpath:
            nstack['basedirpath'] = basedirpath
            cstack['basedirpath'] = basedirpath

        self.clientAuthNr = clientAuthNr or SimpleAuthNr()

        self.nodeInBox = deque()
        self.clientInBox = deque()
        self.created = time.perf_counter()

        HasActionQueue.__init__(self)
        NodeStacked.__init__(self, nstack, nodeReg)
        ClientStacked.__init__(self, cstack)
        Motor.__init__(self)
        Propagator.__init__(self)

        self.totalNodes = len(nodeRegistry)
        self.f = getMaxFailures(self.totalNodes)
        self.requiredNumberOfInstances = self.f + 1  # per RBFT
        self.minimumNodes = (2 * self.f) + 1  # minimum for a functional pool

        self.txnStore = TransactionStore()

        # Stores which protocol instance is master
        self._masterInst = None  # type: Optional[int]

        self.replicas = []  # type: List[replica.Replica]

        self.instanceChanges = InstanceChanges()

        self.viewNo = 0                             # type: int

        self.rank = self.getRank(self.name, nodeRegistry)

        self.elector = None  # type: PrimaryDecider

        self.forwardedRequests = set()  # type: Set[Tuple[(str, int)]]

        self.monitor = Monitor(.9, 60, 5)

        # Requests that are to be given to the replicas by the node. Each
        # element of the list is a deque for the replica with number equal to
        # its index in the list and each element of the deque is a named tuple
        self.msgsToReplicas = []  # type: List[deque]

        # Requests that are to be given to the elector by the node
        self.msgsToElector = deque()

        nodeRoutes = [(Propagate, self.processPropagate),
                      (InstanceChange, self.processInstanceChange)]

        nodeRoutes.extend((msgTyp, self.sendToElector) for msgTyp in
                          [Nomination, Primary, Reelection])

        nodeRoutes.extend((msgTyp, self.sendToReplica) for msgTyp in
                          [PrePrepare, Prepare, Commit])

        self.nodeMsgRouter = Router(*nodeRoutes)

        self.clientMsgRouter = Router((Request,
                                       self.processRequest))

        self.perfCheckFreq = 10

        self._schedule(self.checkPerformance, self.perfCheckFreq)

        self.clientBlacklister = SimpleBlacklister(
            self.name + CLIENT_BLACKLISTER_SUFFIX)  # type: Blacklister

        self.nodeBlacklister = SimpleBlacklister(
            self.name + NODE_BLACKLISTER_SUFFIX)  # type: Blacklister

        # BE CAREFUL HERE
        # This controls which message types are excluded from signature
        # verification. These are still subject to RAET's signature verification
        # but client signatures will not be checked on these. Expressly
        # prohibited from being in this is ClientRequest and Propagation,
        # which both require client signature verification
        self.authnWhitelist = (Nomination, Primary, Reelection,
                               Batch,
                               PrePrepare, Prepare,
                               Commit, InstanceChange)
        self.addReplicas()

    def start(self):
        oldstatus = self.status
        super().start()
        if oldstatus in Status.going():
            logger.info("{} is already {}, so start has no effect".
                        format(self, self.status.name))
        else:
            self.startNodestack()
            self.startClientstack()

            self.elector = self.newPrimaryDecider()

            # if first time running this node
            if not self.nodestack.remotes:
                logger.info("{} first time running; waiting for key sharing..."
                            "".format(self))
            else:
                self.bootstrap()

    @staticmethod
    def getRank(name: str, allNames: Sequence[str]):
        return sorted(allNames).index(name)

    def newPrimaryDecider(self):
        if self.primaryDecider:
            return self.primaryDecider
        else:
            return primary_elector.PrimaryElector(self)

    @property
    def masterInst(self):
        """
        Return the index of the replica that belongs to the master protocol
        instance
        """
        return self._masterInst

    @masterInst.setter
    def masterInst(self, value):
        """
        Set the value of masterProt to the specified value
        """
        self._masterInst = value

    @property
    def nonMasterInsts(self):
        """
        Return the list of replicas that don't belong to the master protocol
        instance
        """
        return [i for i in range(len(self.replicas)) if i != self.masterInst]

    @property
    def nodeCount(self) -> int:
        """
        The plus one is for this node, for example, if this node has three
        connections, then there would be four total nodes
        :return: number of connected nodes this one
        """
        return len(self._conns) + 1

    def onStopping(self):
        """
        Actions to be performed on stopping the node.

        - Close the UDP socket of the nodestack
        """
        if self.nodestack:
            self.nodestack.close()
            self.nodestack = None
        if self.clientstack:
            self.clientstack.close()
            self.clientstack = None
        self.reset()
        self.logstats()

    def reset(self):
        logger.info("{} reseting...".format(self), extra={"cli": False})
        self.nextCheck = 0
        self.aqStash.clear()
        self.actionQueue.clear()
        self.elector = None

    async def prod(self, limit: int=None) -> int:
        """
        This function is executed by the node each time it gets its share of
        CPU time from the event loop.

        :param limit: the number of items to be serviced in this attempt
        :return: total number of messages serviced by this node
        """
        await self.serviceLifecycle()
        c = 0
        if self.status is not Status.stopped:
            c += await self.serviceNodeMsgs(limit)
            c += await self.serviceReplicas(limit)
            c += await self.serviceClientMsgs(limit)
            c += self._serviceActions()
            c += await self.serviceElector()
            self.flushOutBoxes()
        return c

    async def serviceReplicas(self, limit) -> int:
        """
        Execute `serviceReplicaMsgs`, `serviceReplicaOutBox` and
        `serviceReplicaInBox` with `limit` number of messages. See the
        respective functions for more information.

        :param limit: the maximum number of messages to process
        :return: the sum of messages successfully processed by
        serviceReplicaMsgs, serviceReplicaInBox and serviceReplicaOutBox
        """
        a = self.serviceReplicaMsgs(limit)
        b = self.serviceReplicaOutBox(limit)
        c = await self.serviceReplicaInBox(limit)
        return a + b + c

    async def serviceNodeMsgs(self, limit: int) -> int:
        """
        Process `limit` number of messages from the nodeInBox.

        :param limit: the maximum number of messages to process
        :return: the number of messages successfully processed
        """
        n = await self.nodestack.service(limit)
        await self.processNodeInBox()
        return n

    async def serviceClientMsgs(self, limit: int) -> int:
        """
        Process `limit` number of messages from the clientInBox.

        :param limit: the maximum number of messages to process
        :return: the number of messages successfully processed
        """
        c = await self.clientstack.service(limit)
        await self.processClientInBox()
        return c

    async def serviceElector(self) -> int:
        """
        Service the elector's inBox, outBox and action queues.

        :return: the number of messages successfully serviced
        """
        if not self.isReady():
            return 0
        o = self.serviceElectorOutBox()
        i = await self.serviceElectorInbox()
        a = self.elector._serviceActions()
        return o + i + a

    def onConnsChanged(self, newConns: Set[str], staleConns: Set[str]):
        """
        A series of operations to perform once a connection count has changed.

        - Set f to max number of failures this system can handle.
        - Set status to one of started, started_hungry or starting depending on
            the number of protocol instances.
        - Check protocol instances. See `checkProtocolInstaces()`

        """
        if self.isGoing():
            if self.nodeCount >= self.totalNodes:
                self.status = Status.started
                self.stopKeySharing()
            elif self.nodeCount >= self.minimumNodes:
                self.status = Status.started_hungry
            else:
                self.status = Status.starting
        self.elector.nodeCount = self.nodeCount
        if self.isReady():
            self.checkInstances()
            if isinstance(self.elector, PrimaryElector):
                msgs = self.elector.getElectionMsgsForLaggedNodes()
                for n in newConns:
                    self.sendElectionMsgsToLaggedNode(n, msgs)

    def sendElectionMsgsToLaggedNode(self, nodeName: str, msgs: List[Any]):
        rid = self.nodestack.getRemote(nodeName).uid
        for msg in msgs:
            logger.debug("{} sending election message {} to lagged node {}"
                                     .format(self, msg, nodeName))
            self.send(msg, rid)

    def bootstrap(self, forced: bool=None) -> None:
        if forced is None:
            forced = False
        super().bootstrap(forced=forced)
        if not forced:
            logger.debug("{} unforced bootstrap just run, so waiting to run "
                         "it again".format(self))
            # if it wasn't forced, then run it again in 3 seconds to
            # make sure we connected to all
            wait = min(3, len(self.nodeReg) / 4)
            self._schedule(partial(self.bootstrap, forced=True), wait)

    def _statusChanged(self, old: Status, new: Status) -> None:
        """
        Perform some actions based on whether this node is ready or not.

        :param old: the previous status
        :param new: the current status
        """
        pass

    def checkInstances(self) -> None:
        """
        Check if this node has the minimum required number of protocol
        instances, i.e. f+1. If not, add a replica. If no election is in
        progress, this node will try to nominate one of its replicas as primary.
        This method is called whenever a connection with a  new node is
        established.
        """
        logger.debug("{} choosing to start election on the basis of count {} "
                     "and nodes {}".format(self, self.nodeCount, self.conns))
        self._schedule(self.decidePrimaries)

    def addReplicas(self):
        while len(self.replicas) < self.requiredNumberOfInstances:
            self.addReplica()

    def decidePrimaries(self):
        """
        Choose the primary replica for each protocol instance in the system
        using a PrimaryDecider.
        """
        self.elector.decidePrimaries()

    def createReplica(self, instId: int, isMaster: bool) -> 'replica.Replica':
        """
        Create a new replica with the specified parameters.
        This is a convenience method used to create replicas from a node
        instead of passing in replicas in the Node's constructor.

        :param protNo: protocol instance number
        :param isMaster: does this replica belong to the master protocol
            instance?
        :return: a new instance of Replica
        """
        return replica.Replica(self, instId, isMaster)

    def addReplica(self):
        """
        Create and add a new replica to this node.
        If this is the first replica on this node, it will belong to the Master
        protocol instance.
        """
        instId = len(self.replicas)
        if len(self.replicas) == 0:
            self.masterInst = 0
            isMaster = True
            instDesc = "master"
        else:
            isMaster = False
            instDesc = "backup"
        replica = self.createReplica(instId, isMaster)
        self.replicas.append(replica)
        self.msgsToReplicas.append(deque())
        self.monitor.addInstance()
        logger.info("{} added replica {} to instance {} ({})".
                    format(self, replica, instId, instDesc),
                    extra={"cli": True})
        return replica

    def serviceReplicaMsgs(self, limit: int=None) -> int:
        """
        Process `limit` number of replica messages.
        Here processing means appending to replica inbox.

        :param limit: the maximum number of replica messages to process
        :return: the number of replica messages processed
        """
        msgCount = 0
        for idx, replicaMsgs in enumerate(self.msgsToReplicas):
            while replicaMsgs and (not limit or msgCount < limit):
                msgCount += 1
                msg = replicaMsgs.popleft()
                self.replicas[idx].inBox.append(msg)
        return msgCount

    def serviceReplicaOutBox(self, limit: int=None) -> int:
        """
        Process `limit` number of replica messages.
        Here processing means appending to replica inbox.

        :param limit: the maximum number of replica messages to process
        :return: the number of replica messages processed
        """
        msgCount = 0
        for replica in self.replicas:
            while replica.outBox and (not limit or msgCount < limit):
                msgCount += 1
                msg = replica.outBox.popleft()
                if isinstance(msg, (PrePrepare,
                                    Prepare,
                                    Commit)):
                    self.send(msg)
                elif isinstance(msg, Ordered):
                    self.processOrdered(msg)
                else:
                    logger.error("Received msg {} and don't know how to "
                                 "handle it".format(msg))
        return msgCount

    async def serviceReplicaInBox(self, limit: int=None):
        """
        Process `limit` number of messages in the replica inbox for each replica
        on this node.

        :param limit: the maximum number of replica messages to process
        :return: the number of replica messages processed successfully
        """
        msgCount = 0
        for replica in self.replicas:
            msgCount += await replica.serviceQueues(limit)
        return msgCount

    def serviceElectorOutBox(self, limit: int=None) -> int:
        """
        Service at most `limit` number of messages from the elector's outBox.

        :return: the number of messages successfully serviced.
        """
        msgCount = 0
        while self.elector.outBox and (not limit or msgCount < limit):
            msgCount += 1
            msg = self.elector.outBox.popleft()
            if isinstance(msg, (Nomination, Primary, Reelection)):
                self.send(msg)
            elif isinstance(msg, BlacklistMsg):
                nodeName = getattr(msg, f.NODE_NAME.nm)
                code = getattr(msg, f.SUSP_CODE.nm)
                self.reportSuspiciousNode(nodeName, code=code)
            else:
                logger.error("Received msg {} and don't know how to handle it".
                             format(msg))
        return msgCount

    async def serviceElectorInbox(self, limit: int=None) -> int:
        """
        Service at most `limit` number of messages from the elector's outBox.

        :return: the number of messages successfully serviced.
        """
        msgCount = 0
        while self.msgsToElector and (not limit or msgCount < limit):
            msgCount += 1
            msg = self.msgsToElector.popleft()
            self.elector.inBox.append(msg)
        await self.elector.serviceQueues(limit)
        return msgCount

    @property
    def hasPrimary(self) -> bool:
        """
        Does this node have a primary replica?

        :return: whether this node has a primary
        """
        return any(replica.isPrimary for replica in self.replicas)

    @property
    def primaryReplicaNo(self) -> Optional[int]:
        """
        Return the index of the primary or None if there's no primary among the
        replicas on this node.

        :return: index of the primary
        """
        for idx, replica in enumerate(self.replicas):
            if replica.isPrimary:
                return idx
        return None

    def isValidNodeMsg(self, msg) -> bool:
        """
        Return whether the node message is valid.

        :param msg: the node message to validate
        """
        if msg.instId >= len(self.msgsToReplicas):
            self.discard(msg, "non-existent protocol instance {}"
                         .format(msg.instId))
            return False
        return True

    def sendToReplica(self, msg, frm):
        """
        Send the message to the intended replica.

        :param msg: the message to send
        :param frm: the name of the node which sent this `msg`
        """
        if self.isValidNodeMsg(msg):
            self.msgsToReplicas[msg.instId].append((msg, frm))

    def sendToElector(self, msg, frm):
        """
        Send the message to the intended elector.

        :param msg: the message to send
        :param frm: the name of the node which sent this `msg`
        """
        if self.isValidNodeMsg(msg):
            logger.debug("{} sending message to elector: {}".
                         format(self, (msg, frm)))
            self.msgsToElector.append((msg, frm))

    def handleOneNodeMsg(self, wrappedMsg):
        """
        Validate and process one message from a node.

        :param wrappedMsg: Tuple of message and the name of the node that sent
        the message
        """
        try:
            vmsg = self.validateNodeMsg(wrappedMsg)
            if vmsg:
                self.unpackNodeMsg(*vmsg)
        except SuspiciousNode as ex:
            msg, frm = wrappedMsg
            exc = ex.__cause__ if ex.__cause__ else ex
            self.reportSuspiciousNode(frm, exc)
            self.discard(msg, exc)
        except Exception as ex:
            msg, frm = wrappedMsg
            self.discard(msg, ex)

    def validateNodeMsg(self, wrappedMsg):
        """
        Validate another node's message sent to this node.

        :param wrappedMsg: Tuple of message and the name of the node that sent
        the message
        :return: Tuple of message from node and name of the node
        """
        msg, frm = wrappedMsg
        if self.isNodeBlacklisted(frm):
            self.discard(msg, "received from blacklisted node {}"
                         .format(frm), logger.info)
            return None

        op = msg.pop(OP_FIELD_NAME, None)
        if not op:
            raise MissingNodeOp
        cls = TaggedTuples.get(op, None)
        if not cls:
            raise InvalidNodeOp(op)
        try:
            cMsg = cls(**msg)
        except Exception as ex:
            raise InvalidNodeMsg from ex
        try:
            self.verifySignature(cMsg)
        except Exception as ex:
            raise SuspiciousNode from ex
        logger.debug("{} received node message from {}: {}".
                     format(self, frm, cMsg),
                     extra={"cli": False})
        return cMsg, frm

    def unpackNodeMsg(self, msg, frm) -> None:
        """
        If the message is a batch message validate each message in the batch,
        otherwise add the message to the node's inbox.

        :param msg: a node message
        :param frm: the name of the node that sent this `msg`
        """
        if isinstance(msg, Batch):
            for m in msg.messages:
                self.handleOneNodeMsg((m, frm))
        else:
            self.postToNodeInBox(msg, frm)

    def postToNodeInBox(self, msg, frm):
        """
        Append the message to the node inbox

        :param msg: a node message
        :param frm: the name of the node that sent this `msg`
        """
        self.nodeInBox.append((msg, frm))

    async def processNodeInBox(self):
        """
        Process the messages in the node inbox asynchronously.
        """
        while self.nodeInBox:
            m = self.nodeInBox.popleft()
            try:
                await self.nodeMsgRouter.handle(m)
            except SuspiciousNode as ex:
                _, frm = m
                exc = ex.__cause__ if ex.__cause__ else ex
                self.reportSuspiciousNode(frm, exc)
                self.discard(m, ex)

    def handleOneClientMsg(self, wrappedMsg):
        """
        Validate and process a client message

        :param wrappedMsg: a message from a client
        """
        try:
            vmsg = self.validateClientMsg(wrappedMsg)
            if vmsg:
                self.unpackClientMsg(*vmsg)
        except SuspiciousClient as ex:
            msg, frm = wrappedMsg
            exc = ex.__cause__ if ex.__cause__ else ex
            self.reportSuspiciousClient(frm, exc)
            self.discard(wrappedMsg, exc)

    def validateClientMsg(self, wrappedMsg):
        """
        Validate a message sent by a client.

        :param wrappedMsg: a message from a client
        :return: Tuple of clientMessage and client address
        """
        msg, frm = wrappedMsg
        if self.isClientBlacklisted(frm):
            self.discard(msg, "received from blacklisted client {}"
                         .format(frm), logger.info)
            return None

        if all(attr in msg.keys() for attr in [OPERATION, 'clientId', 'reqId']):
            cls = Request
        elif OP_FIELD_NAME in msg:
            op = msg.pop(OP_FIELD_NAME)
            cls = TaggedTuples.get(op, None)
            if not cls:
                raise InvalidClientOp(op)
            if cls is not Batch:
                raise InvalidClientMsgType(cls)
        else:
            raise InvalidClientRequest
        # don't check for signature on Batches from clients, signatures will
        # be checked on the individual messages when they are unpacked
        try:
            cMsg = cls(**msg)
        except Exception as ex:
            raise InvalidClientRequest from ex
        try:
            self.verifySignature(cMsg)
        except Exception as ex:
            raise SuspiciousClient from ex
        logger.trace("{} received CLIENT message: {}".
                     format(self.clientstack.name, cMsg))
        return cMsg, frm

    def unpackClientMsg(self, msg, frm):
        """
        If the message is a batch message validate each message in the batch,
        otherwise add the message to the node's clientInBox.

        :param msg: a client message
        :param frm: the clientId of the client that sent this `msg`
        """
        if isinstance(msg, Batch):
            for m in msg.messages:
                self.handleOneClientMsg((m, frm))
        else:
            self.postToClientInBox(msg, frm)

    def postToClientInBox(self, msg, frm):
        """
        Append the message to the node's clientInBox

        :param msg: a client message
        :param frm: the name of the node that sent this `msg`
        """
        self.clientInBox.append((msg, frm))

    async def processClientInBox(self):
        """
        Process the messages in the node's clientInBox asynchronously.
        All messages in the inBox have already been validated, including
        signature check.
        """
        while self.clientInBox:
            m = self.clientInBox.popleft()
            req, frm = m
            logger.debug("{} processing {} request {}".
                         format(self.clientstack.name, frm, req.reqId),
                         extra={"cli": True})
            await self.clientMsgRouter.handle(m)

    async def processRequest(self, request: Request, frm: str):
        """
        Handle a REQUEST from the client.
        If the request has already been executed, the node re-sends the reply to
        the client. Otherwise, the node acknowledges the client request, adds it
        to its list of client requests, and sends a PROPAGATE to the
        remaining nodes.

        :param request: the REQUEST from the client
        :param frm: the clientId of the client that sent this REQUEST
        """
        logger.debug("Node {} received client request: {}".
                     format(self.name, request))

        # If request is already processed(there is a reply for the request in
        # the node's transaction store then return the reply from the
        # transaction store)
        txnId = self.txnStore.isRequestAlreadyProcessed(request)
        if txnId:
            logger.debug("{} returning REPLY from already processed "
                         "REQUEST: {}".format(self, request))
            reply = self.txnStore.transactions[txnId]
            self.transmitToClient(reply, request.clientId)
        else:
            self.transmitToClient(RequestAck(request.reqId), frm)
            # If not already got the propagate request(PROPAGATE) for the
            # corresponding client request(REQUEST)
            self.recordAndPropagate(request)

    # noinspection PyUnusedLocal
    async def processPropagate(self, msg: Propagate, frm):
        """
        Process one propagateRequest sent to this node asynchronously

        - If this propagateRequest hasn't been seen by this node, then broadcast
        it to all nodes after verifying the the signature.
        - Add the client to blacklist if its signature is invalid

        :param msg: the propagateRequest
        :param frm: the name of the node which sent this `msg`
        """
        logger.debug("Node {} received propagated request: {}".
                     format(self.name, msg))
        reqDict = msg.request
        request = Request(**reqDict)

        # TODO it appears this signature validation is redundant
        # try:
        #     self.verifySignature(reqDict)
        # except Exception as ex:
        #     raise SuspiciousNode from ex

        self.requests.addPropagate(request, frm)

        self.propagate(request)
        self.tryForwarding(request)

    def processOrdered(self, ordered: Ordered, retryNo: int = 0):
        """
        Process and orderedRequest.

        Execute client request with retries if client request hasn't yet reached
        this node but corresponding PROPAGATE, PRE-PREPARE, PREPARE and
        COMMIT request did

        :param ordered: an orderedRequest
        :param retryNo: the retry number used in recursion
        :return: True if successful, None otherwise
        """
        instId, viewNo, clientId, reqId, digest = tuple(ordered)

        self.monitor.requestOrdered(clientId,
                                    reqId,
                                    instId,
                                    byMaster=(instId == self.masterInst))

        # Only the request ordered by master protocol instance are executed by
        # the client
        if instId == self.masterInst:
            key = (clientId, reqId)
            if key in self.requests:
                req = self.requests[key].request
                self.executeRequest(viewNo, req)
                logger.debug("Node {} executing client request {} {}".
                             format(self.name, clientId, reqId))
            # If the client request hasn't reached the node but corresponding
            # PROPAGATE, PRE-PREPARE, PREPARE and COMMIT request did,
            # then retry 3 times
            elif retryNo < 3:
                retryNo += 1
                p = partial(self.processOrdered, ordered, retryNo)
                self._schedule(p, random.randint(2, 4))

                logger.debug("Node {} retrying executing client request {} {}".
                             format(self.name, clientId, reqId))
            return True
        else:
            logger.trace("{} got ordered request from backup replica".
                         format(self))

    def processInstanceChange(self, instChg: InstanceChange, frm: str) -> None:
        """
        Validate and process an instance change request.

        :param instChg: the instance change request
        :param frm: the name of the node that sent this `msg`
        """
        logger.debug("Node {} received instance change request: {} from {}".
                     format(self, instChg, frm))
        if instChg.viewNo < self.viewNo:
            self.discard(instChg, "Received instance change request with view"
                                    " no {} which is less than its view no {}"
                         .format(instChg.viewNo, self.viewNo), logger.debug)
        else:
            if not self.instanceChanges.hasView(instChg.viewNo):
                if self.isMasterSlow:
                    self.instanceChanges.addVote(instChg.viewNo, frm)
                    self.sendInstanceChange(instChg.viewNo)
                else:
                    self.discard(instChg, "received instance change message "
                                            "from {} but did not find the "
                                            "master to be slow"
                                 .format(frm), logger.debug)
                    return
            else:
                if self.instanceChanges.hasVoteFromSender(instChg.viewNo, frm):
                    logger.debug("{} already received instance change request "
                                 "with view no {} from {}"
                                 .format(self, instChg.viewNo, frm))
                    return
                else:
                    self.instanceChanges.addVote(instChg.viewNo, frm)

                    if self.canViewChange(instChg.viewNo):
                        logger.debug("{} initiating a view change with view "
                                     "no {}".format(self, self.viewNo))
                        self.startViewChange(instChg.viewNo)
                    else:
                        logger.trace("{} cannot initiate a view change".
                                     format(self))

    def checkPerformance(self):
        """
        Check if master instance is slow and send an instance change request.
        """
        logger.debug("{} checking its performance".format(self))
        if self.masterInst is not None:
            if self.isMasterSlow:
                logger.debug("{}'s master has lower performance than backups. "
                             "Sending an instance change with viewNo {}".
                             format(self, self.viewNo))
                self.sendInstanceChange(self.viewNo)
            else:
                logger.debug("{}'s master has higher performance than backups".
                             format(self))

        self._schedule(self.checkPerformance, self.perfCheckFreq)

    @property
    def isMasterSlow(self):
        """
        Return whether the master instance is slow.
        """
        return self.masterInst is not None and \
               (self.lowMasterThroughput or
                self.highMasterReqLatency or
                self.highMasterAvgReqLatency)

    @property
    def lowMasterThroughput(self):
        """
        Return whether the throughput of the master instance is greater than the
        acceptable threshold
        """
        masterThrp, backupThrp = self.monitor.getThroughputs(self.masterInst)
        logger.debug("{}'s master throughput is {}, average backup throughput "
                     "is {}".format(self, masterThrp, backupThrp))

        # Backup throughput may be 0 so moving ahead only if it is not 0
        if not backupThrp or masterThrp is None:
            return False

        r = masterThrp / backupThrp
        logger.debug("{}'s ratio of master throughput to average backup "
                     "throughput is {} and Delta is {}".
                     format(self, r, self.monitor.Delta))
        if r < self.monitor.Delta:
            logger.debug("{}'s master throughput is lower.".format(self))
            return True
        else:
            logger.debug("{}'s master throughput is ok.".format(self))
            return False

    @property
    def highMasterReqLatency(self):
        """
        Return whether the request latency of the master instance is greater
        than the acceptable threshold
        """
        r = any([lat > self.monitor.Lambda for lat
                 in self.monitor.masterReqLatencies.values()])
        if r:
            logger.debug("{} found master's latency to be higher than the "
                         "threshold for some or all requests.".format(self))
        else:
            logger.debug("{} found master's latency to be lower than the "
                         "threshold for all requests.".format(self))
        return r

    @property
    def highMasterAvgReqLatency(self):
        """
        Return whether the average request latency of the master instance is
        greater than the acceptable threshold
        """
        avgLatM = self.monitor.getAvgLatency(self.masterInst)
        avgLatB = self.monitor.getAvgLatency(*self.nonMasterInsts)
        logger.debug("{}'s master's avg request latency is {} and backup's "
                     "avg request latency is {} ".
                     format(self, avgLatM, avgLatB))
        r = False
        # If latency of the master for any client is greater than that of
        # backups by more than the threshold `Omega`, then a view change
        # needs to happen
        for cid, lat in avgLatB.items():
            if avgLatM[cid] - lat > self.monitor.Omega:
                r = True
                break
        if r:
            logger.debug("{} found difference between master's and backups's "
                         "avg latency to be higher than the threshold".
                         format(self))
        else:
            logger.debug("{} found difference between master's and backups's "
                         "avg latency to be lower than the threshold".
                         format(self))
        return r

    def executeRequest(self, viewNo: int, req: Request) -> None:
        """
        Execute the REQUEST sent to this Node

        :param viewNo: the view number (See glossary)
        :param req: the client REQUEST
        """
        reply = self.generateReply(viewNo, req)
        self.transmitToClient(reply, req.clientId)
        txnId = reply.result['txnId']
        self.txnStore.addToProcessedRequests(req.clientId, req.reqId,
                                             txnId, reply)
        asyncio.ensure_future(self.txnStore.reply(
            clientId=req.clientId, reply=reply, txnId=txnId))

    def sendInstanceChange(self, viewNo: int):
        """
        Broadcast an instance change request to all the remaining nodes

        :param viewNo: the view number when the instance change is requested
        """
        self.send(InstanceChange(viewNo))
        self.instanceChanges.addVote(viewNo, self.name)

    @property
    def quorum(self) -> int:
        r"""
        Return the quorum of this RBFT system. Equal to :math:`2f + 1`.
        """
        return (2 * self.f) + 1

    def canViewChange(self, proposedViewNo: int) -> bool:
        """
        Return whether there's quorum for view change for the proposed view
        number
        """
        return self.instanceChanges.hasQuorum(proposedViewNo, self.f)

    def startViewChange(self, newViewNo: int):
        """
        Trigger the view change process.

        :param newViewNo: the new view number after view change.
        """
        self.viewNo = newViewNo + 1
        logger.debug("{} resetting monitor stats after view change".
                     format(self))
        self.monitor.reset()

        # Now communicate the view change to the elector which will
        # contest primary elections across protocol all instances
        self.elector.viewChanged(self.viewNo)

    def verifySignature(self, msg) -> bool:
        """
        Validate the signature of the request
        Note: Batch is whitelisted because the inner messages are checked

        :param msg: a message requiring signature verification
        :return: None; raises an exception if the signature is not valid
        """
        if isinstance(msg, self.authnWhitelist):
            return  # whitelisted message types rely on RAET for authn
        if isinstance(msg, Propagate):
            typ = 'propagate '
            req = msg.request
        else:
            typ = ''
            req = msg

        if not isinstance(req, Mapping):
            req = msg.__getstate__()

        identifier = self.clientAuthNr.authenticate(req)
        logger.debug("{} authenticated {} signature on {}request {}".
                     format(self, identifier, typ, req['reqId']),
                     extra={"cli": True})

    def generateReply(self,
                      viewNo: int,
                      req: Request) -> Reply:
        """
        Return a new clientReply created using the viewNo, request and the
        computed txnId of the request

        :param viewNo: the view number (See glossary)
        :param req: the REQUEST
        :return: a clientReply generated from the request
        """
        logger.debug("{} replying request {}".format(self, req))
        txnId = sha256("{}{}{}".format(viewNo, req.clientId, req.reqId).
                       encode('utf-8')).hexdigest()
        return Reply(viewNo,
                     req.reqId,
                     {"txnId": txnId})

    def startKeySharing(self, timeout=60):
        """
        Start key sharing till the timeout is reached.
        Other nodes will be able to join this node till the timeout is reached.

        :param timeout: the time till which key sharing is active
        """
        if self.nodestack.keep.auto != AutoMode.never:
            logger.info("{} already key sharing".format(self),
                        extra={"cli": "LOW_STATUS"})
        else:
            logger.info("{} starting key sharing".format(self),
                        extra={"cli": "STATUS"})
            self.nodestack.keep.auto = AutoMode.once
            self._schedule(partial(self.stopKeySharing, timedOut=True), timeout)
            self.bootstrap()

    def stopKeySharing(self, timedOut=False):
        """
        Stop key sharing, i.e don't allow any more nodes to join this node.
        """
        if self.nodestack.keep.auto != AutoMode.never:
            if timedOut:
                logger.info("{} key sharing timed out; was not able to "
                            "connect to {}".
                            format(self, ", ".join(self.notConnectedNodes())),
                            extra={"cli": "WARNING"})
            else:
                logger.info("{} completed key sharing".format(self),
                            extra={"cli": "STATUS"})
            self.nodestack.keep.auto = AutoMode.never

    def reportSuspiciousNode(self, nodeName: str, reason=None, code: int=None):
        logger.warning("{} suspicion raised on node {} for {}; "
                       "doing nothing for now. Suspicion code is {}".
                       format(self, nodeName, reason, code))
        if isinstance(reason, InvalidSignature):
            self.blacklistNode(nodeName, reason=reason, code=100)

        if code in self.suspicions:
            self.blacklistNode(nodeName,
                               reason=self.suspicions[code],
                               code=code)

    def reportSuspiciousClient(self, clientName: str, reason):
        logger.warning("{} suspicion raised on client {} for {}; "
                       "doing nothing for now".
                       format(self, clientName, reason))
        self.blacklistClient(clientName)

    def isClientBlacklisted(self, clientName: str):
        return self.clientBlacklister.isBlacklisted(clientName)

    def blacklistClient(self, clientName: str, reason: str=None):
        msg = "{} blacklisting client {}".format(self, clientName)
        if reason:
            msg += " for reason {}".format(reason)
        logger.debug(msg)
        self.clientBlacklister.blacklist(clientName)

    def isNodeBlacklisted(self, nodeName: str):
        return self.nodeBlacklister.isBlacklisted(nodeName)

    def blacklistNode(self, nodeName: str, reason: str=None, code: int=None):
        msg = "{} blacklisting node {}".format(self, nodeName)
        if reason:
            msg += " for reason {}".format(reason)
        if code:
            msg += " for code {}".format(code)
        logger.debug(msg)
        self.nodeBlacklister.blacklist(nodeName)

    def __enter__(self):
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def logstats(self):
        lines = []
        l = lines.append
        l("node {} current stats".format(self))
        l("--------------------------------------------------------")
        l("node inbox size         : {}".format(len(self.nodeInBox)))
        l("client inbox size       : {}".
                    format(len(self.clientInBox)))
        l("age (seconds)           : {}".
                    format(time.perf_counter() - self.created))
        l("next check for reconnect: {}".
                    format(time.perf_counter() - self.nextCheck))
        l("node connections        : {}".format(self._conns))
        l("f                       : {}".format(self.f))
        l("master instance         : {}".format(self._masterInst))
        l("replicas                : {}".format(len(self.replicas)))
        l("view no                 : {}".format(self.viewNo))
        l("rank                    : {}".format(self.rank))
        l("msgs to replicas        : {}".
                    format(len(self.msgsToReplicas)))
        l("msgs to elector         : {}".
                    format(len(self.msgsToElector)))
        l("action queue            : {} {}".
                    format(len(self.actionQueue), id(self.actionQueue)))
        l("action queue stash      : {} {}".
                    format(len(self.aqStash), id(self.aqStash)))

        logger.info("\n".join(lines), extra={"cli": False})

CLIENT_STACK_SUFFIX = "C"
CLIENT_BLACKLISTER_SUFFIX = "BLC"
NODE_BLACKLISTER_SUFFIX = "BLN"
NodeDetail = NamedTuple("NodeDetail", [
    ("ha", HA),
    ("cliname", str),
    ("cliha", HA)])
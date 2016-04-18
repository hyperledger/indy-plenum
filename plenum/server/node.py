import asyncio
import os
import random
import time
from binascii import hexlify
from collections import deque, OrderedDict
from functools import partial
from hashlib import sha256
from typing import Dict, Any, Mapping, Iterable, List, NamedTuple, Optional, \
    Sequence, Set
from typing import Tuple

from libnacl.encode import base64_decode
from raet.raeting import AutoMode

from ledger.immutable_store.ledger import Ledger
from ledger.immutable_store.store import F
from ledger.immutable_store.merkle import CompactMerkleTree
from plenum.common.exceptions import SuspiciousNode, SuspiciousClient, \
    MissingNodeOp, InvalidNodeOp, InvalidNodeMsg, InvalidClientMsgType, \
    InvalidClientOp, InvalidClientRequest, InvalidSignature, BaseExc, \
    InvalidClientMessageException, RaetKeysNotFoundException as REx
from plenum.common.motor import Motor
from plenum.common.raet import isLocalKeepSetup, initRemoteKeep
from plenum.common.request_types import Request, Propagate, \
    Reply, Nomination, OP_FIELD_NAME, TaggedTuples, Primary, \
    Reelection, PrePrepare, Prepare, Commit, \
    Ordered, RequestAck, InstanceChange, Batch, OPERATION, BlacklistMsg, f, \
    RequestNack
from plenum.common.stacked import HA, ClientStacked
from plenum.common.stacked import NodeStacked
from plenum.common.startable import Status
from plenum.common.transaction_store import TransactionStore
from plenum.common.txn import TXN_TYPE, NEW_NODE, DATA, ALIAS, NODE_IP, NODE_PORT, \
    PUBKEY, TARGET_NYM, CLIENT_PORT, CLIENT_IP, NEW_STEWARD, NEW_CLIENT, STEWARD, \
    CLIENT, TXN_ID, ClientBootStrategy
from plenum.common.util import getMaxFailures, MessageProcessor, getlogger, \
    getConfig
from plenum.server import primary_elector
from plenum.server import replica
from plenum.server.blacklister import Blacklister
from plenum.server.blacklister import SimpleBlacklister
from plenum.server.client_authn import ClientAuthNr, SimpleAuthNr
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.instances import Instances
from plenum.server.models import InstanceChanges
from plenum.server.monitor import Monitor
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.primary_elector import PrimaryElector
from plenum.server.propagator import Propagator
from plenum.server.router import Router
from plenum.server.suspicion_codes import Suspicions
from plenum.storage.storage import Storage


logger = getlogger()


class Node(HasActionQueue, NodeStacked, ClientStacked, Motor,
           Propagator, MessageProcessor):
    """
    A node in a plenum system. Nodes communicate with each other via the RAET
    protocol. https://github.com/saltstack/raet
    """

    suspicions = {s.code: s.reason for s in Suspicions.getList()}

    def __init__(self,
                 name: str,
                 nodeRegistry: Dict[str, HA]=None,
                 clientAuthNr: ClientAuthNr=None,
                 ha: HA=None,
                 cliname: str=None,
                 cliha: HA=None,
                 basedirpath: str=None,
                 primaryDecider: PrimaryDecider = None,
                 opVerifiers: Iterable[Any]=None,
                 storage: Storage=None,
                 config=None):

        """
        Create a new node.

        :param nodeRegistry: names and host addresses of all nodes in the pool
        :param clientAuthNr: client authenticator implementation to be used
        :param basedirpath: path to the base directory used by `nstack` and
            `cstack`
        :param primaryDecider: the mechanism to be used to decide the primary
        of a protocol instance
        """
        self.config = config or getConfig()
        self.ensureKeysAreSetup(name, basedirpath)
        self.opVerifiers = opVerifiers or []
        self.clientBootStrategy = self.config.clientBootStrategy
        self.clientAuthNr = clientAuthNr or self.defaultAuthNr()
        self.poolTansactionsStore = Ledger(CompactMerkleTree(),
                                            dataDir=basedirpath,
                                            fileName=self.config.poolTransactionsFile)
        if not nodeRegistry:
            nstack, cstack, nodeReg = self.bootstrapFromPoolTxns(
                name, basedirpath=basedirpath)
        else:
            nstack, nodeReg = self.getNodeStackParams(name, nodeRegistry, ha,
                                                      basedirpath=basedirpath)
            cstack = self.getClientStackParams(name, nodeRegistry,
                                               cliname=cliname, cliha=cliha,
                                               basedirpath=basedirpath)
        self.basedirpath = basedirpath
        self.allNodeNames = list(nodeReg.keys())

        self.primaryDecider = primaryDecider

        self.txnStore = storage if storage is not None else TransactionStore()

        self.nodeInBox = deque()
        self.clientInBox = deque()
        self.created = time.perf_counter()

        HasActionQueue.__init__(self)
        NodeStacked.__init__(self, nstack, nodeReg)
        ClientStacked.__init__(self, cstack)
        Motor.__init__(self)
        Propagator.__init__(self)

        self.totalNodes = len(nodeReg)
        self.f = getMaxFailures(self.totalNodes)
        self.requiredNumberOfInstances = self.f + 1  # per RBFT
        self.minimumNodes = (2 * self.f) + 1  # minimum for a functional pool

        self.replicas = []  # type: List[replica.Replica]

        self.instanceChanges = InstanceChanges()

        self.viewNo = 0                             # type: int

        self.rank = self.getRank(self.name, nodeReg)

        self.elector = None  # type: PrimaryDecider

        self.forwardedRequests = set()  # type: Set[Tuple[(str, int)]]

        self.instances = Instances()

        self.monitor = Monitor(self.name,
                               Delta=.8, Lambda=60, Omega=5,
                               instances=self.instances)

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

        # Map of request identifier to client name. Used for
        # dispatching the processed requests to the correct client remote
        self.clientIdentifiers = {}     # Dict[str, str]

    def bootstrapFromPoolTxns(self, selfName, basedirpath):
        if not os.path.isfile(os.path.join(basedirpath,
                                           self.config.poolTransactionsFile)):
            raise FileNotFoundError("Pool transactions file not found")
        nstack = None
        cstack = None
        nodeReg = OrderedDict()
        for _, txn in self.poolTansactionsStore.getAllTxn().items():
            if txn[TXN_TYPE] == NEW_NODE:
                verkey, pubkey = hexlify(base64_decode(txn[TARGET_NYM].encode())), \
                                 txn[DATA][PUBKEY]
                nodeName = txn[DATA][ALIAS]
                nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
                nodeReg[nodeName] = HA(*nodeHa)
                if nodeName == selfName:
                    nstack = dict(name=selfName,
                                  ha=HA('0.0.0.0', txn[DATA][NODE_PORT]),
                                  main=True,
                                  auto=AutoMode.never)
                    cstack = dict(name=nodeName + CLIENT_STACK_SUFFIX,
                                  ha=HA('0.0.0.0', txn[DATA][CLIENT_PORT]),
                                  main=True,
                                  auto=AutoMode.always)

                    if basedirpath:
                        nstack['basedirpath'] = basedirpath
                        cstack['basedirpath'] = basedirpath
                else:
                    try:
                        initRemoteKeep(selfName, nodeName, basedirpath, pubkey, verkey)
                    except Exception as ex:
                        print(ex)
            elif txn[TXN_TYPE] in (NEW_STEWARD, NEW_CLIENT) \
                    and self.clientBootStrategy == ClientBootStrategy.PoolTxn:
                self.addNewRole(txn)

        return nstack, cstack, nodeReg

    def getNodeStackParams(self, name, nodeRegistry: Dict[str, HA], ha: HA=None,
                           basedirpath: str=None):
        me = nodeRegistry[name]

        self.allNodeNames = list(nodeRegistry.keys())
        if isinstance(me, NodeDetail):
            sha = me.ha
            nodeReg = {k: v.ha for k, v in nodeRegistry.items()}
        else:
            sha = me if isinstance(me, HA) else HA(*me[0])
            nodeReg = {k:  v if isinstance(v, HA) else HA(*v[0])
                       for k, v in nodeRegistry.items()}
        if not ha:  # pull it from the registry
            ha = sha

        nstack = dict(name=name,
                  ha=ha,
                  main=True,
                  auto=AutoMode.never)

        if basedirpath:
            nstack['basedirpath'] = basedirpath

        return nstack, nodeReg

    def getClientStackParams(self, name, nodeRegistry: Dict[str, HA], cliname,
                             cliha, basedirpath):
        me = nodeRegistry[name]

        if isinstance(me, NodeDetail):
            sha = me.ha
            scliname = me.cliname
            scliha = me.cliha
        else:
            sha = me if isinstance(me, HA) else HA(*me[0])
            scliname = None
            scliha = None

        if not cliname:  # default to the name plus the suffix
            cliname = scliname if scliname else name + CLIENT_STACK_SUFFIX
        if not cliha:  # default to same ip, port + 1
            cliha = scliha if scliha else HA(sha[0], sha[1] + 1)

        cstack = dict(name=cliname,
                      ha=cliha,
                      main=True,
                      auto=AutoMode.always)

        if basedirpath:
            cstack['basedirpath'] = basedirpath

        return cstack

    def start(self, loop):
        oldstatus = self.status
        super().start(loop)
        if oldstatus in Status.going():
            logger.info("{} is already {}, so start has no effect".
                        format(self, self.status.name))
        else:
            self.txnStore.start(loop)
            self.startNodestack()
            self.startClientstack()

            self.elector = self.newPrimaryDecider()

            # if first time running this node
            if not self.nodestack.remotes:
                logger.info("{} first time running; waiting for key sharing..."
                            "".format(self))
            else:
                self.maintainConnections()

    @staticmethod
    def getRank(name: str, allNames: Sequence[str]):
        return sorted(allNames).index(name)

    def newPrimaryDecider(self):
        if self.primaryDecider:
            return self.primaryDecider
        else:
            return primary_elector.PrimaryElector(self)

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
        self.conns.clear()
        # Stop the txn store
        self.txnStore.stop()

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
            c += self.serviceReplicas(limit)
            c += await self.serviceClientMsgs(limit)
            c += self._serviceActions()
            c += await self.serviceElector()
            self.flushOutBoxes()
        return c

    def serviceReplicas(self, limit) -> int:
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
        c = self.serviceReplicaInBox(limit)
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
                logger.debug("{} has msgs {} for new nodes {}".format(self,
                                                                      msgs, newConns))
                for n in newConns:
                    self.sendElectionMsgsToLaggedNode(n, msgs)

    def sendElectionMsgsToLaggedNode(self, nodeName: str, msgs: List[Any]):
        rid = self.nodestack.getRemote(nodeName).uid
        for msg in msgs:
            logger.debug("{} sending election message {} to lagged node {}".
                         format(self, msg, nodeName))
            self.send(msg, rid)

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
                elif isinstance(msg, Exception):
                    self.processEscalatedException(msg)
                else:
                    logger.error("Received msg {} and don't know how to "
                                 "handle it".format(msg))
        return msgCount

    def serviceReplicaInBox(self, limit: int=None):
        """
        Process `limit` number of messages in the replica inbox for each replica
        on this node.

        :param limit: the maximum number of replica messages to process
        :return: the number of replica messages processed successfully
        """
        msgCount = 0
        for replica in self.replicas:
            msgCount += replica.serviceQueues(limit)
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
            # TODO should we raise suspicion here?
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
            self.reportSuspiciousNodeEx(ex)
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
        # TODO why must we catch and raise? Is there a way to know earlier that
        # the signature exception is suspicious? If so, how suspicious?
        except BaseExc as ex:
            raise SuspiciousNode(frm, ex, cMsg) from ex  # TODO are both needed?
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
                self.reportSuspiciousNodeEx(ex)
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
            self.handleInvalidClientMsg(exc, wrappedMsg)
        except InvalidClientMessageException as ex:
            self.handleInvalidClientMsg(ex, wrappedMsg)

    def handleInvalidClientMsg(self, ex, wrappedMsg):
        _, frm = wrappedMsg
        exc = ex.__cause__ if ex.__cause__ else ex
        reason = "client request invalid: {} {}". \
            format(exc.__class__.__name__, exc)
        self.transmitToClient(RequestNack(ex.reqId, reason), frm)
        self.discard(wrappedMsg, ex, logger.warning, cliOutput=True)

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

        if all(attr in msg.keys()
               for attr in [OPERATION, f.IDENTIFIER.nm, f.REQ_ID.nm]):
            self.checkValidOperation(msg[f.IDENTIFIER.nm],
                                     msg[f.REQ_ID.nm],
                                     msg[OPERATION])
            cls = Request
        elif OP_FIELD_NAME in msg:
            op = msg.pop(OP_FIELD_NAME)
            cls = TaggedTuples.get(op, None)
            if not cls:
                raise InvalidClientOp(op, msg.get(f.REQ_ID.nm))
            if cls is not Batch:
                raise InvalidClientMsgType(cls, msg.get(f.REQ_ID.nm))
        else:
            raise InvalidClientRequest
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
        :param frm: the name of the client that sent this `msg`
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
            try:
                await self.clientMsgRouter.handle(m)
            except InvalidClientMessageException as ex:
                self.handleInvalidClientMsg(ex, m)

    async def processRequest(self, request: Request, frm: str):
        """
        Handle a REQUEST from the client.
        If the request has already been executed, the node re-sends the reply to
        the client. Otherwise, the node acknowledges the client request, adds it
        to its list of client requests, and sends a PROPAGATE to the
        remaining nodes.

        :param request: the REQUEST from the client
        :param frm: the name of the client that sent this REQUEST
        """
        logger.debug("Node {} received client request: {}".
                     format(self.name, request))

        # If request is already processed(there is a reply for the request in
        # the node's transaction store then return the reply from the
        # transaction store)
        if request.identifier not in self.clientIdentifiers:
            self.clientIdentifiers[request.identifier] = frm

        reply = await self.getReplyFor(request.identifier, request.reqId)
        if reply:
            logger.debug("{} returning REPLY from already processed "
                         "REQUEST: {}".format(self, request))
            self.transmitToClient(reply, frm)
        else:
            await self.checkRequestAuthorized(request)
            self.transmitToClient(RequestAck(request.reqId), frm)
            # If not already got the propagate request(PROPAGATE) for the
            # corresponding client request(REQUEST)
            self.recordAndPropagate(request, frm)

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

        clientName = msg.senderClient

        if request.identifier not in self.clientIdentifiers:
            self.clientIdentifiers[request.identifier] = clientName

        self.requests.addPropagate(request, frm)

        self.propagate(request, clientName)
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

        instId, viewNo, identifier, reqId, digest, ppTime = tuple(ordered)

        self.monitor.requestOrdered(identifier,
                                    reqId,
                                    instId,
                                    byMaster=(instId == self.instances.masterId))

        # Only the request ordered by master protocol instance are executed by
        # the client
        if instId == self.instances.masterId:
            key = (identifier, reqId)
            if key in self.requests:
                req = self.requests[key].request
                self.executeRequest(viewNo, ppTime, req)
                logger.debug("Node {} executing client request {} {}".
                             format(self.name, identifier, reqId))
            # If the client request hasn't reached the node but corresponding
            # PROPAGATE, PRE-PREPARE, PREPARE and COMMIT request did,
            # then retry 3 times
            elif retryNo < 3:
                retryNo += 1
                p = partial(self.processOrdered, ordered, retryNo)
                self._schedule(p, random.randint(2, 4))

                logger.debug("Node {} retrying executing client request {} {}".
                             format(self.name, identifier, reqId))
            return True
        else:
            logger.trace("{} got ordered request from backup replica".
                         format(self))

    def processEscalatedException(self, ex):
        """
        Process an exception escalated from a Replica
        """
        if isinstance(ex, SuspiciousNode):
            self.reportSuspiciousNodeEx(ex)
        else:
            raise RuntimeError("unhandled replica-escalated exception") from ex

    def processInstanceChange(self, instChg: InstanceChange, frm: str) -> None:
        """
        Validate and process an instance change request.

        :param instChg: the instance change request
        :param frm: the name of the node that sent this `msg`
        """
        logger.debug("Node {} received instance change request: {} from {}".
                     format(self, instChg, frm))
        if instChg.viewNo < self.viewNo:
            self.discard(instChg,
                         "Received instance change request with view no {} "
                         "which is less than its view no {}".
                         format(instChg.viewNo, self.viewNo), logger.debug)
        else:
            if not self.instanceChanges.hasView(instChg.viewNo):
                if self.monitor.isMasterDegraded():
                    self.instanceChanges.addVote(instChg.viewNo, frm)
                    self.sendInstanceChange(instChg.viewNo)
                else:
                    self.discard(instChg,
                                 "received instance change message from {} but "
                                 "did not find the master to be slow".
                                 format(frm), logger.debug)
                    return
            else:
                if self.instanceChanges.hasInstChngFrom(instChg.viewNo, frm):
                    self.reportSuspiciousNode(frm,
                                              code=Suspicions.DUPLICATE_INST_CHNG)
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
        :returns True if master performance is OK, otherwise False
        """
        logger.debug("{} checking its performance".format(self))
        self._schedule(self.checkPerformance, self.perfCheckFreq)

        if self.instances.masterId is not None:
            if self.monitor.isMasterDegraded():
                logger.info("{} master has lower performance than backups. "
                            "Sending an instance change with viewNo {}".
                            format(self, self.viewNo))
                logger.info("{} metrics for monitor: {}".
                            format(self, self.monitor.prettymetrics))
                self.sendInstanceChange(self.viewNo)
                return False
            else:
                logger.debug("{}'s master has higher performance than backups".
                             format(self))
        return True

    def executeRequest(self, viewNo: int, ppTime: float, req: Request) -> None:
        """
        Execute the REQUEST sent to this Node

        :param viewNo: the view number (See glossary)
        :param ppTime: the time at which PRE-PREPARE was sent
        :param req: the client REQUEST
        """
        op = req.operation
        if op.get(TXN_TYPE) in (NEW_NODE, NEW_STEWARD, NEW_CLIENT):
            self.executePoolTxnRequest(viewNo, ppTime, req)
        else:
            self.doCustomAction(viewNo, ppTime, req)

    def executePoolTxnRequest(self, viewNo, ppTime, req):
        reply = self.generateReply(viewNo, ppTime, req)
        op = req.operation
        if op[TXN_TYPE] == NEW_NODE:
            self.addNewNodeAndConnect(op)
        elif op[TXN_TYPE] in (NEW_STEWARD, NEW_CLIENT) and \
                        self.clientBootStrategy == ClientBootStrategy.PoolTxn:
            self.addNewRole(op)
        asyncio.ensure_future(self.poolTansactionsStore.append(
            identifier=req.identifier, reply=reply, txnId=reply.result[TXN_ID]))

    # TODO: Find a better name for the function
    def doCustomAction(self, viewNo, ppTime, req):
        reply = self.generateReply(viewNo, ppTime, req)
        asyncio.ensure_future(self.txnStore.append(
            identifier=req.identifier, reply=reply, txnId=reply.result[TXN_ID]))
        self.transmitToClient(reply, self.clientIdentifiers[req.identifier])

    async def getReplyFor(self, identifier, reqId):
        return await self.txnStore.get(identifier, reqId)

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
        number and its view is less than or equal to the proposed view
        """
        return self.instanceChanges.hasQuorum(proposedViewNo, self.f) and \
               self.viewNo <= proposedViewNo

    def startViewChange(self, proposedViewNo: int):
        """
        Trigger the view change process.

        :param proposedViewNo: the new view number after view change.
        """
        self.viewNo = proposedViewNo + 1
        logger.debug("{} resetting monitor stats after view change".
                     format(self))
        self.monitor.reset()

        # Now communicate the view change to the elector which will
        # contest primary elections across protocol all instances
        self.elector.viewChanged(self.viewNo)

    def verifySignature(self, msg):
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
                      ppTime: float,
                      req: Request) -> Reply:
        """
        Return a new clientReply created using the viewNo, request and the
        computed txnId of the request

        :param viewNo: the view number (See glossary)
        :param ppTime: the time at which PRE-PREPARE was sent
        :param req: the REQUEST
        :return: a Reply generated from the request
        """
        logger.debug("{} replying request {}".format(self, req))
        txnId = sha256("{}{}{}".format(viewNo, req.identifier, req.reqId).
                       encode('utf-8')).hexdigest()
        return Reply(viewNo, req.reqId, {"txnId": txnId, "time": ppTime})

    # TODO: Remove this
    def startKeySharing(self, timeout=60):
        """
        Start key sharing till the timeout is reached.
        Other nodes will be able to join this node till the timeout is reached.

        :param timeout: the time till which key sharing is active
        """
        if self.isKeySharing:
            logger.info("{} already key sharing".format(self),
                        extra={"cli": "LOW_STATUS"})
        else:
            logger.info("{} starting key sharing".format(self),
                        extra={"cli": "STATUS"})
            self.nodestack.keep.auto = AutoMode.always
            self._schedule(partial(self.stopKeySharing, timedOut=True), timeout)

            # remove any unjoined remotes
            for r in self.nodestack.nameRemotes.values():
                if not r.joined:
                    logger.debug("{} removing unjoined remote {}"
                                 .format(self, r))
                    self.nodestack.removeRemote(r)

            # if just starting, then bootstrap
            force = time.perf_counter() - self.created > 5
            self.maintainConnections(force=force)

    # TODO: Remove this
    def stopKeySharing(self, timedOut=False):
        """
        Stop key sharing, i.e don't allow any more nodes to join this node.
        """
        if self.isKeySharing:
            if timedOut:
                logger.info("{} key sharing timed out; was not able to "
                            "connect to {}".
                            format(self, ", ".join(self.notConnectedNodes())),
                            extra={"cli": "WARNING"})
            else:
                logger.info("{} completed key sharing".format(self),
                            extra={"cli": "STATUS"})
            self.nodestack.keep.auto = AutoMode.never

    def checkValidOperation(self, clientId, reqId, msg):
        if self.opVerifiers:
            try:
                for v in self.opVerifiers:
                    v.verify(msg)
            except Exception as ex:
                raise InvalidClientRequest(clientId, reqId) from ex

    async def checkRequestAuthorized(self, request):
        """
        Subclasses can implement this method to throw an
        UnauthorizedClientRequest if the request is not authorized.

        If a request makes it this far, the signature has been verified to match
        the identifier.
        """
        pass

    def defaultAuthNr(self):
        return SimpleAuthNr()

    @staticmethod
    def ensureKeysAreSetup(name, baseDir):
        if not isLocalKeepSetup(name, baseDir):
            raise REx(REx.reason)

    def addNewRole(self, txn):
        role = STEWARD if txn[TXN_TYPE] == NEW_STEWARD else CLIENT
        roleName = txn[DATA][ALIAS]
        verkey = base64_decode(txn[TARGET_NYM].encode())
        pubkey = txn[DATA][PUBKEY]
        self.clientAuthNr.addClient(roleName,
                                    verkey=verkey,
                                    pubkey=pubkey,
                                    role=role)

    def addNewNodeAndConnect(self, txn):
        verkey, pubkey = hexlify(base64_decode(txn[TARGET_NYM].encode())), \
                         txn[DATA][PUBKEY]
        nodeName = txn[DATA][ALIAS]
        nodeHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT])
        try:
            initRemoteKeep(self.name, nodeName, self.basedirpath, pubkey,
                           verkey)
        except Exception as ex:
            logger.debug("Exception whle initializing keep for remote {}".
                         format(ex))
        self.nodestack.nodeReg[nodeName] = HA(*nodeHa)

    def reportSuspiciousNodeEx(self, ex: SuspiciousNode):
        self.reportSuspiciousNode(ex.node, ex.reason, ex.code, ex.offendingMsg)

    def reportSuspiciousNode(self,
                             nodeName: str,
                             reason=None,
                             code: int=None,
                             offendingMsg=None):
        logger.warning("{} suspicion raised on node {} for {}; suspicion code "
                       "is {}".format(self, nodeName, reason, code))
        # TODO need a more general solution here
        if code == InvalidSignature.code:
            self.blacklistNode(nodeName,
                               reason=InvalidSignature.reason,
                               code=InvalidSignature.code)

        if code in self.suspicions:
            self.blacklistNode(nodeName,
                               reason=self.suspicions[code],
                               code=code)
        if offendingMsg:
            self.discard(offendingMsg, reason, logger.warning)

    def reportSuspiciousClient(self, clientName: str, reason):
        logger.warning("{} suspicion raised on client {} for {}; "
                       "doing nothing for now".
                       format(self, clientName, reason))
        self.blacklistClient(clientName)

    def isClientBlacklisted(self, clientName: str):
        return self.clientBlacklister.isBlacklisted(clientName)

    def blacklistClient(self, clientName: str, reason: str=None, code: int=None):
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

    def verifyMerkleProof(self, merkleData, identifier):
        keys = [F.serialNo.name, F.STH.name, F.auditInfo.name]
        assert all(x in list(merkleData.keys()) for x in keys), \
            "Required data missing for operation VERIF_TXN"
        # TODO Implement the Merkle Proof verification
        self.transmitToClient(True, remoteName=self.clientIdentifiers[identifier])

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
        l("master instance         : {}".format(self.instances.masterId))
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

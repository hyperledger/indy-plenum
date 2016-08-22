import asyncio
import heapq
import importlib
import logging
import math
import operator
import os
import random
import time
from base64 import b64encode
from collections import deque, defaultdict, OrderedDict
from functools import partial
from hashlib import sha256
from typing import Dict, Any, Mapping, Iterable, List, Optional, \
    Sequence, Set
from typing import Tuple

import pyorient
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.serializers.compact_serializer import CompactSerializer
from ledger.stores.file_hash_store import FileHashStore
from ledger.stores.hash_store import HashStore
from ledger.stores.memory_hash_store import MemoryHashStore
from ledger.util import F
from raet.raeting import AutoMode

from plenum.client.signer import Signer
from plenum.common.exceptions import SuspiciousNode, SuspiciousClient, \
    MissingNodeOp, InvalidNodeOp, InvalidNodeMsg, InvalidClientMsgType, \
    InvalidClientOp, InvalidClientRequest, InvalidSignature, BaseExc, \
    InvalidClientMessageException, RaetKeysNotFoundException as REx
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.motor import Motor
from plenum.common.plugin_helper import loadPlugins
from plenum.common.raet import isLocalKeepSetup
from plenum.common.stacked import NodeStack, ClientStack
from plenum.common.startable import Status, Mode
from plenum.common.txn import TXN_TYPE, TXN_ID, TXN_TIME, POOL_TXN_TYPES
from plenum.common.types import Request, Propagate, \
    Reply, Nomination, OP_FIELD_NAME, TaggedTuples, Primary, \
    Reelection, PrePrepare, Prepare, Commit, \
    Ordered, RequestAck, InstanceChange, Batch, OPERATION, BlacklistMsg, f, \
    RequestNack, CLIENT_BLACKLISTER_SUFFIX, NODE_BLACKLISTER_SUFFIX, HA, \
    NODE_SECONDARY_STORAGE_SUFFIX, NODE_PRIMARY_STORAGE_SUFFIX, HS_ORIENT_DB, \
    HS_FILE, NODE_HASH_STORE_SUFFIX, LedgerStatus, ConsistencyProof, \
    CatchupReq, CatchupRep, NodeRegForClient, CLIENT_STACK_SUFFIX, \
    PLUGIN_TYPE_VERIFICATION, PLUGIN_TYPE_PROCESSING
from plenum.common.util import getMaxFailures, MessageProcessor, getlogger, \
    getConfig, runWithLoop
from plenum.persistence.orientdb_hash_store import OrientDbHashStore
from plenum.persistence.orientdb_store import OrientDbStore
from plenum.persistence.secondary_storage import SecondaryStorage
from plenum.persistence.storage import Storage, initStorage
from plenum.server import primary_elector
from plenum.server import replica
from plenum.server.blacklister import Blacklister
from plenum.server.blacklister import SimpleBlacklister
from plenum.server.client_authn import ClientAuthNr, SimpleAuthNr
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.instances import Instances
from plenum.server.models import InstanceChanges
from plenum.server.monitor import Monitor
from plenum.server.plugin.has_plugin_loader_helper import PluginLoaderHelper
from plenum.server.pool_manager import HasPoolManager, TxnPoolManager, \
    RegistryPoolManager
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.primary_elector import PrimaryElector
from plenum.server.propagator import Propagator
from plenum.server.router import Router
from plenum.server.suspicion_codes import Suspicions

logger = getlogger()


class Node(HasActionQueue, Motor,
           Propagator, MessageProcessor, HasFileStorage,
           HasPoolManager, PluginLoaderHelper):
    """
    A node in a plenum system. Nodes communicate with each other via the
    RAET protocol. https://github.com/saltstack/raet
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
                 pluginPaths: Iterable[str]=None,
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
        self.created = time.perf_counter()
        self.name = name
        self.config = config or getConfig()
        self.basedirpath = basedirpath or config.baseDir
        self.dataDir = self.config.dataDir or "data/nodes"
        HasFileStorage.__init__(self, name, baseDir=self.basedirpath,
                                dataDir=self.dataDir)
        self.ensureKeysAreSetup(name, basedirpath)
        self.opVerifiers = self.getPluginsByType(pluginPaths,
                                                 PLUGIN_TYPE_VERIFICATION)
        self.reqProcessors = self.getPluginsByType(pluginPaths,
                                                   PLUGIN_TYPE_PROCESSING)

        self.clientAuthNr = clientAuthNr or self.defaultAuthNr()

        self.requestExecuter = defaultdict(lambda: self.doCustomAction)

        HasPoolManager.__init__(self, nodeRegistry, ha, cliname, cliha)
        if isinstance(self.poolManager, RegistryPoolManager):
            self.mode = Mode.discovered
        else:
            self.mode = None  # type: Optional[Mode]

        self.nodeReg = self.poolManager.nodeReg

        # noinspection PyCallingNonCallable
        self.nodestack = self.nodeStackClass(self.poolManager.nstack,
                                   self.handleOneNodeMsg,
                                   self.nodeReg)
        self.nodestack.onConnsChanged = self.onConnsChanged

        # noinspection PyCallingNonCallable
        self.clientstack = self.clientStackClass(self.poolManager.cstack,
                                       self.handleOneClientMsg)

        self.cliNodeReg = self.poolManager.cliNodeReg

        HasActionQueue.__init__(self)
        Motor.__init__(self)
        Propagator.__init__(self)

        self.primaryDecider = primaryDecider

        self.nodeInBox = deque()
        self.clientInBox = deque()

        self.setF()

        self.replicas = []  # type: List[replica.Replica]

        self.instanceChanges = InstanceChanges()

        self.viewNo = 0                             # type: int

        self.rank = self.getRank(self.name, self.nodeReg)

        self.elector = None  # type: PrimaryDecider

        self.forwardedRequests = set()  # type: Set[Tuple[(str, int)]]

        self.instances = Instances()

        self.monitor = Monitor(self.name,
                               Delta=self.config.DELTA,
                               Lambda=self.config.LAMBDA,
                               Omega=self.config.OMEGA,
                               instances=self.instances,
                               pluginPaths=pluginPaths)

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

        nodeRoutes.extend([
            (LedgerStatus, self.processLedgerStatus),
            (ConsistencyProof, self.processConsistencyProof),
            (CatchupReq, self.processCatchupReq),
            (CatchupRep, self.processCatchupRep)
        ])

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
                               Commit, InstanceChange, LedgerStatus,
                               ConsistencyProof, CatchupReq, CatchupRep)

        # Map of request identifier to client name. Used for
        # dispatching the processed requests to the correct client remote
        self.clientIdentifiers = {}     # Dict[str, str]

        self.hashStore = self.getHashStore(self.name)
        self.primaryStorage = storage or self.getPrimaryStorage()
        self.secondaryStorage = self.getSecondaryStorage()

        # Domain ledger statuses received while this node has still
        # not discovered its peers
        self.stashedLedgerStatuses = deque()

        # List of 2 elements with first element corresponding to pool ledger and
        # the second corresponding to domain ledger.
        # Each set tracks which nodes claim that this node's ledger status is ok
        # , if a quorum of nodes (2f+1) say its up to date then mark the catchup
        #  process as completed
        self.ledgerStatusOk = [set(), set()]

        # Consistency proofs received in process of catching up.
        # The first element of the list is the dictionary of consistency proofs
        # received for the pool transaction ledger and second element is the
        # dictionary of consistency proofs received for the domain transaction
        # ledger. For each dictionary key is the node name and value is a
        # consistency proof.
        self.recvdConsistencyProofs = [{}, {}]  # type: List[Dict[str,
        # ConsistencyProof]]

        self.catchUpTill = [None, None]

        # Catchup replies that need to be applied to the ledger. First element
        # of the list is a list of transactions that need to be applied to the
        # pool transaction ledger and the second element is the list of
        # transactions that need to be applied to the domain transaction ledger
        self.receivedCatchUpReplies = [[], []]

        # Ordered requests received from replicas while the node was not
        # participating
        self.stashedOrderedReqs = deque()

        # Set of (identifier, reqId) of all transactions that were received
        # while catching up. Used to detect overlap between stashed requests
        # and received replies while catching up.
        self.reqsFromCatchupReplies = set()

        self.addReplicas()

        self.msgsForFutureReplicas = {}

        loadPlugins(basedirpath)

    def __repr__(self):
        return self.name

    # noinspection PyAttributeOutsideInit
    def setF(self):
        nodeNames = set(self.nodeReg.keys())
        self.allNodeNames = nodeNames.union({self.name, })
        self.totalNodes = len(self.allNodeNames)
        self.f = getMaxFailures(self.totalNodes)
        self.requiredNumberOfInstances = self.f + 1  # per RBFT
        self.minimumNodes = (2 * self.f) + 1  # minimum for a functional pool

    @property
    def poolLedger(self):
        return self.poolManager.poolTxnStore if isinstance(self.poolManager,
                                                           TxnPoolManager) \
            else None

    @property
    def domainLedger(self):
        return self.primaryStorage

    @property
    def poolLedgerStatus(self):
        return LedgerStatus(0, self.poolManager.txnSeqNo,
                            self.poolManager.merkleRootHash) \
            if self.poolLedger else None

    @property
    def domainLedgerStatus(self):
        return LedgerStatus(1, self.primaryStorage.size,
                            self.primaryStorage.root_hash)

    @property
    def isParticipating(self):
        return self.mode == Mode.participating

    @property
    def nodeStackClass(self) -> NodeStack:
        return NodeStack

    @property
    def clientStackClass(self) -> ClientStack:
        return ClientStack

    def getPrimaryStorage(self):
        """
        This is usually an implementation of Ledger
        """
        if self.config.primaryStorage is None:
            fields = OrderedDict([
                (f.IDENTIFIER.nm, (str, str)),
                (f.REQ_ID.nm, (str, int)),
                (TXN_ID, (str, str)),
                (TXN_TIME, (str, float)),
                (TXN_TYPE, (str, str)),
            ])
            return Ledger(CompactMerkleTree(hashStore=self.hashStore),
                dataDir=self.getDataLocation(),
                serializer=CompactSerializer(fields=fields),
                fileName=self.config.domainTransactionsFile)
        else:
            return initStorage(self.config.primaryStorage,
                               name=self.name+NODE_PRIMARY_STORAGE_SUFFIX,
                               dataDir=self.getDataLocation(),
                               config=self.config)

    def getHashStore(self, name) -> HashStore:
        """
        Create and return a hashStore implementation based on configuration
        """
        hsConfig = self.config.hashStore['type'].lower()
        if hsConfig == HS_FILE:
            return FileHashStore(dataDir=self.getDataLocation(),
                                 fileNamePrefix=NODE_HASH_STORE_SUFFIX)
        elif hsConfig == HS_ORIENT_DB:
            if hasattr(self, '_orientDbStore'):
                store = self._orientDbStore
            else:
                store = self._getOrientDbStore(name,
                                               pyorient.DB_TYPE_GRAPH)
            return OrientDbHashStore(store)
        else:
            return MemoryHashStore()

    def getSecondaryStorage(self) -> SecondaryStorage:
        """
        Create and return an instance of secondaryStorage to be
        used by this Node.
        """
        if self.config.secondaryStorage:
            return initStorage(self.config.secondaryStorage,
                               name=self.name+NODE_SECONDARY_STORAGE_SUFFIX,
                               dataDir=self.getDataLocation(),
                               config=self.config)
        else:
            return SecondaryStorage(txnStore=None,
                                    primaryStorage=self.primaryStorage)

    def _getOrientDbStore(self, name, dbType) -> OrientDbStore:
        """
        Helper method that creates an instance of OrientdbStore.

        :param name: name of the orientdb database
        :param dbType: orientdb database type
        :return: orientdb store
        """
        self._orientDbStore = OrientDbStore(
            user=self.config.OrientDB["user"],
            password=self.config.OrientDB["password"],
            dbName=name,
            dbType=dbType,
            storageType=pyorient.STORAGE_TYPE_PLOCAL)
        return self._orientDbStore

    def start(self, loop):
        oldstatus = self.status
        super().start(loop)
        if oldstatus in Status.going():
            logger.info("{} is already {}, so start has no effect".
                        format(self, self.status.name))
        else:
            self.primaryStorage.start(loop)
            self.nodestack.start()
            self.clientstack.start()

            self.elector = self.newPrimaryDecider()

            # if first time running this node
            if not self.nodestack.remotes:
                logger.info("{} first time running; waiting for key sharing..."
                            "".format(self))
            else:
                self.nodestack.maintainConnections()
        if isinstance(self.poolManager, RegistryPoolManager):
            self.mode = Mode.discovered
        else:
            self.mode = Mode.starting

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
        return len(self.nodestack.conns) + 1

    def onStopping(self):
        """
        Actions to be performed on stopping the node.

        - Close the UDP socket of the nodestack
        """
        self.reset()
        # TODO: The next 2 lines should be part of reset
        self.nodestack.conns.clear()
        # TODO: Should `self.clientstack.conns` be cleared too
        # self.clientstack.conns.clear()

        self.logstats()
        # Stop the txn store
        self.primaryStorage.stop()

        if self.nodestack.opened:
            self.nodestack.close()
        if self.clientstack.opened:
            self.clientstack.close()

        self.mode = None

    def reset(self):
        logger.info("{} reseting...".format(self), extra={"cli": False})
        self.nodestack.nextCheck = 0
        logger.debug("{} clearing aqStash of size {}".format(self, len(self.aqStash)))
        if self.aqStash:
            logger.debug(self.aqStash)
        self.aqStash.clear()
        logger.debug(
            "{} clearing actionQueue of size {}".format(self, len(self.actionQueue)))
        if self.actionQueue:
            logger.debug(self.actionQueue)
        self.actionQueue.clear()
        self.elector = None

    async def prod(self, limit: int=None) -> int:
        """.opened
        This function is executed by the node each time it gets its share of
        CPU time from the event loop.

        :param limit: the number of items to be serviced in this attempt
        :return: total number of messages serviced by this node
        """
        if self.isGoing():
            await self.nodestack.serviceLifecycle()
            newClients = self.clientstack.serviceClientStack()
            if newClients:
                self.newClientsConnected(newClients)
        c = 0
        if self.status is not Status.stopped:
            c += await self.serviceNodeMsgs(limit)
            c += await self.serviceReplicas(limit)
            c += await self.serviceClientMsgs(limit)
            c += self._serviceActions()
            c += self.monitor._serviceActions()
            c += await self.serviceElector()
            self.nodestack.flushOutBoxes()
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
        b = await self.serviceReplicaOutBox(limit)
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

    def onConnsChanged(self, joined: Set[str], left: Set[str]):
        """
        A series of operations to perform once a connection count has changed.

        - Set f to max number of failures this system can handle.
        - Set status to one of started, started_hungry or starting depending on
            the number of protocol instances.
        - Check protocol instances. See `checkInstances()`

        """
        if self.isGoing():
            if self.nodeCount == self.totalNodes:
                self.status = Status.started
                self.stopKeySharing()
            elif self.nodeCount >= self.minimumNodes:
                self.status = Status.started_hungry
            else:
                self.status = Status.starting
        self.elector.nodeCount = self.nodeCount

        if self.isReady():
            self.checkInstances()
            # TODO: Should we only send election messages when lagged or
            # otherwise too?
            if isinstance(self.elector, PrimaryElector) and joined:
                msgs = self.elector.getElectionMsgsForLaggedNodes()
                logger.debug("{} has msgs {} for new nodes {}".format(self, msgs,
                                                                     joined))
                for n in joined:
                    self.sendElectionMsgsToLaggingNode(n, msgs)
                    # Communicate current view number if any view change
                    # happened to the connected node
                    if self.viewNo > 0:
                        logger.debug("{} communicating view number {} to {}"
                                     .format(self, self.viewNo-1, n))
                        rid = self.nodestack.getRemote(n).uid
                        self.send(InstanceChange(self.viewNo-1), rid)

        # Send ledger status whether ready (connected to enough nodes) or not
        for n in joined:
            self.sendPoolLedgerStatus(n)
            # Send the domain ledger status only when it has discovered enough
            # peers otherwise very few peers will know that this node is lagging
            # behind and it will not receive sufficient consistency proofs to
            # verify the exact state of the ledger.
            if self.mode in (Mode.discovered, Mode.participating):
                self.sendDomainLedgerStatus(n)

    def newNodeJoined(self, nodeName: str):
        self.setF()
        if self.addReplicas():
            self.decidePrimaries()
        # TODO: Should tell the client after the new node has synced up its
        # ledgers
        self.sendNodeHaToClients(nodeName)

    def newClientsConnected(self, newClients: Set[str]):
        for client in newClients:
            # if client in self.clientAuthNr.clients:
            logger.debug("sending noderegs to client {}".format(client))
            msg = NodeRegForClient({self.getClientStackNameOfNode(nm):
                                        self.getClientStackHaOfNode(nm)
                                    for nm in self.nodestack.connecteds})
            self.transmitToClient(msg, client)

    def sendNodeHaToClients(self, nodeName):
        logger.debug("sending new node {} info to all clients".format(nodeName))
        msg = NodeRegForClient({self.getClientStackNameOfNode(nodeName):
                                    self.getClientStackHaOfNode(nodeName)})
        self.clientstack.transmitToClients(msg,
                                           list(self.clientstack.connectedClients))

    @property
    def clientStackName(self):
        return self.getClientStackNameOfNode(self.name)

    @staticmethod
    def getClientStackNameOfNode(nodeName: str):
        return nodeName + CLIENT_STACK_SUFFIX

    def getClientStackHaOfNode(self, nodeName: str) -> HA:
        return self.cliNodeReg.get(self.getClientStackNameOfNode(nodeName))

    def sendElectionMsgsToLaggingNode(self, nodeName: str, msgs: List[Any]):
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
                     "and nodes {}".format(self, self.nodeCount,
                                           self.nodestack.conns))
        self._schedule(self.decidePrimaries)

    def addReplicas(self):
        newReplicas = 0
        while len(self.replicas) < self.requiredNumberOfInstances:
            self.addReplica()
            newReplicas += 1
        return newReplicas

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

        :param instId: protocol instance number
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
        logger.display("{} added replica {} to instance {} ({})".
                    format(self, replica, instId, instDesc),
                    extra={"cli": True, "demo": True})
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

    async def serviceReplicaOutBox(self, limit: int=None) -> int:
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
                    # Checking for request in received catchup replies as a
                    # request ordering might have started when the node was not
                    # participating but by the time ordering finished, node
                    # might have started participating
                    if self.isParticipating and not self.gotInCatchupReplies(msg):
                        await self.processOrdered(msg)
                    else:
                        logger.debug("{} stashing {}".format(self, msg))
                        self.stashedOrderedReqs.append(msg)
                elif isinstance(msg, Exception):
                    self.processEscalatedException(msg)
                else:
                    logger.error("Received msg {} and don't know how to handle"
                                 "it".format(msg))
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

    def msgHasAcceptableInstId(self, msg) -> bool:
        """
        Return true if the instance id of message corresponds to a correct
        replica.

        :param msg: the node message to validate
        :return:
        """
        instId = getattr(msg, "instId", None)
        if instId >= len(self.msgsToReplicas):
            # TODO: Should probably queue messages for new instances
            self.discard(msg, "non-existent protocol instance {}"
                         .format(instId), logMethod=logging.debug)
            return False
        return True

    def msgHasAcceptableViewNo(self, msg) -> bool:
        """
        Return true if the view no of message corresponds to the current view
        no, or a view no in the past that the replicas know of or a view no in
        the future

        :param msg: the node message to validate
        :return:
        """
        viewNo = getattr(msg, "viewNo", None)
        corrects = []
        for r in self.replicas:
            if not r.primaryNames.keys():
                # Replica has not seen any primary
                corrects.append(True)
            elif viewNo in r.primaryNames.keys():
                # Replica has seen primary with this view no
                corrects.append(True)
            elif viewNo > max(r.primaryNames.keys()):
                # msg for a future view no
                corrects.append(True)
            else:
                corrects.append(False)
        r = all(corrects)
        if not r:
            self.discard(msg, "un-acceptable viewNo {}"
                         .format(viewNo), logMethod=logging.debug)
        return r

    def sendToReplica(self, msg, frm):
        """
        Send the message to the intended replica.

        :param msg: the message to send
        :param frm: the name of the node which sent this `msg`
        """
        if self.msgHasAcceptableInstId(msg) and self.msgHasAcceptableViewNo(msg):
            self.msgsToReplicas[msg.instId].append((msg, frm))

    def sendToElector(self, msg, frm):
        """
        Send the message to the intended elector.

        :param msg: the message to send
        :param frm: the name of the node which sent this `msg`
        """
        if self.msgHasAcceptableInstId(msg) and self.msgHasAcceptableViewNo(
                msg):
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
            else:
                logger.info("{} non validated msg {}".format(self, wrappedMsg))
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
        except BaseExc as ex:
            raise SuspiciousNode(frm, ex, cMsg) from ex
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
            logger.debug("{} processing a batch {}".format(self, msg))
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
        logger.debug("{} appending to nodeinxbox {}".format(self, msg))
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
        reason = "client request invalid: {} {}".\
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
            logger.display("{} processing {} request {}".
                         format(self.clientstack.name, frm, req.reqId),
                         extra={"cli": True})
            try:
                await self.clientMsgRouter.handle(m)
            except InvalidClientMessageException as ex:
                self.handleInvalidClientMsg(ex, m)

    def processLedgerStatus(self, status: LedgerStatus, frm: str):
        logger.debug("{} received ledger status: {} from {}".
                     format(self.name, status, frm))
        # Nodes might not be using pool txn ledger, might be using simple node
        # registries (old approach)
        ledgerStatus = LedgerStatus(*status) if status else None
        ledgerType = getattr(status, f.LEDGER_TYPE.nm)

        if ledgerType == 1 and self.mode in (Mode.starting, Mode.discovering):
            self.stashedLedgerStatuses.append((status, frm))
            return

        # If this node's ledger is ahead of the received ledger status
        if ledgerStatus and self.isLedgerNew(ledgerStatus):
            consistencyProof = self.getConsistencyProof(ledgerStatus)
            rid = self.nodestack.getRemote(frm).uid
            self.send(ConsistencyProof(*consistencyProof), rid)

        if ledgerStatus and not self.isLedgerOld(ledgerStatus):
            # This node's ledger is not older so it will not receive a
            # consistency proof unless the other node processes a transaction
            # post sending this ledger status
            self.recvdConsistencyProofs[ledgerType][frm] = None
            self.ledgerStatusOk[ledgerType].add(frm)
            if len(self.ledgerStatusOk[ledgerType]) == 2*self.f:
                logger.debug("{} found out from {} that its ledger of type {} "
                             "is latest".
                             format(self, self.ledgerStatusOk[ledgerType],
                                    ledgerType))
                self.catchupCompleted(ledgerType)

    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        logger.debug("{} received consistency proof: {} from {}".
                     format(self.name, proof, frm))
        ledgerType = getattr(proof, f.LEDGER_TYPE.nm)
        if self.canProcessConsistencyProof(ledgerType):
            # TODO: Handle case where a malign node can send a consistency
            # proof for a previous ledger status
            self.recvdConsistencyProofs[ledgerType][frm] = \
                ConsistencyProof(*proof)
            canCatchup, catchUpFrm = self.canStartCatchUpProcess(ledgerType)
            if canCatchup:
                self.startCatchUpProcess(ledgerType, catchUpFrm)
        else:
            logger.debug("{} ignoring consistency proof since in {} mode".
                         format(self, self.mode.name))

    def canProcessConsistencyProof(self, ledgerType: int) -> bool:
        if ledgerType == 0:
            # Pool txn ledger so only accept consistency proofs if not started
            # discovering peers
            return self.mode == Mode.starting
        if ledgerType == 1:
            # Domain txn ledger so only accept consistency proofs if not stated
            # catching up with domain ledger but done discovering peers
            return self.mode == Mode.discovered
        return False

    def processCatchupReq(self, req: CatchupReq, frm: str):
        logger.debug("{} received catchup request: {} from {}".
                     format(self.name, req, frm))
        start, end = getattr(req, f.SEQ_NO_START.nm), \
                     getattr(req, f.SEQ_NO_END.nm)
        ledger = self.getLedgerForMsg(req)
        txns = ledger.getAllTxn(start, end)
        logger.debug("{} generating consistency proof: {} from {}".
                     format(self.name, end, ledger.size))
        consProof = [b64encode(p).decode() for p in
                     ledger.tree.consistency_proof(end, ledger.size)]
        rid = self.nodestack.getRemote(frm).uid
        self.send(CatchupRep(getattr(req, f.LEDGER_TYPE.nm), txns, consProof),
                  rid)

    def processCatchupRep(self, rep: CatchupRep, frm: str):
        logger.debug("{} received catchup reply: {} from {}".
                     format(self.name, rep, frm))
        ledgerType = getattr(rep, f.LEDGER_TYPE.nm)
        if self.canProcessCatchupReply(ledgerType):
            # Not relying on a node sending txns in order of sequence no
            txns = sorted([(int(s), t) for (s, t) in
                           getattr(rep, f.TXNS.nm).items()],
                          key=operator.itemgetter(0))
            if txns:
                ledger = self.getLedgerForMsg(rep)
                catchUpReplies = self.receivedCatchUpReplies[ledgerType]
                catchUpReplies = list(heapq.merge(catchUpReplies, txns,
                                                  key=operator.itemgetter(0)))
                numProcessed = self.processCatchupReplies(ledgerType, ledger,
                                                          catchUpReplies)
                self.receivedCatchUpReplies[ledgerType] = catchUpReplies[numProcessed:]
                if getattr(self.catchUpTill[ledgerType], f.SEQ_NO_END.nm) == \
                        ledger.size:
                    self.catchUpTill[ledgerType] = None
                    self.catchupCompleted(ledgerType)

    def processCatchupReplies(self, ledgerType, ledger: Ledger,
                              catchUpReplies: List):
        processed = []
        for seqNo, txn in catchUpReplies:
            if seqNo - ledger.seqNo == 1:
                txn.pop(F.seqNo.name, None)
                ledger.add(txn)
                if ledgerType == 0:
                    self.poolManager.onPoolMembershipChange(txn)
                processed.append(seqNo)
                self.reqsFromCatchupReplies.add((txn.get(f.IDENTIFIER.nm),
                                                 txn.get(f.REQ_ID.nm)))
            else:
                break
        logger.debug("{} processed {} catchup replies with sequence numbers {}"
                     .format(self, len(processed), processed))
        return len(processed)

    def canProcessCatchupReply(self, ledgerType: int) -> bool:
        if ledgerType == 0:
            return self.mode == Mode.discovering
        if ledgerType == 1:
            return self.mode == Mode.syncing
        return False

    # ASSUMING NO MALICIOUS NODES
    # Assuming that all nodes have the same state of the system and no node
    # is lagging behind. So if two new nodes are added in quick succession in a
    # high traffic environment, this logic is faulty
    def canStartCatchUpProcess(self, ledgerType: int):
        recvdConsProof = self.recvdConsistencyProofs[ledgerType]
        # Consider an f value when this node was not connected
        adjustedF = getMaxFailures(len(set(self.allNodeNames)-{self.name,}))
        if len(recvdConsProof) >= 2*adjustedF:
            logger.debug("{} deciding on the basis of CPs {}".format(self,
                                                                     recvdConsProof))
            result = None
            recvdPrf = {}
            # For the case where the node sending the consistency proof is at
            # the same state as this node
            nullProofs = 0
            for nodeName, proof in recvdConsProof.items():
                if proof:
                    start, end = getattr(proof, f.SEQ_NO_START.nm), \
                                 getattr(proof, f.SEQ_NO_END.nm)
                    if (start, end) not in recvdPrf:
                        recvdPrf[(start, end)] = {}
                    key = (getattr(proof, f.OLD_MERKLE_ROOT.nm),
                           getattr(proof, f.NEW_MERKLE_ROOT.nm),
                           tuple(getattr(proof, f.HASHES.nm)))
                    recvdPrf[(start, end)][key] = recvdPrf[(start, end)]. \
                                                      get(key, 0) + 1
                    if recvdPrf[(start, end)][key] > adjustedF:
                        result = proof
                else:
                    logger.debug("{} found proof by {} null".format(self, nodeName))
                    nullProofs += 1

            if nullProofs == len(recvdConsProof):
                return True, None

            return bool(result), result
        logger.debug("{} cannot start catchup since received only {} "
                     "consistency proofs but need at least {}"
                     .format(self, len(recvdConsProof),
                             adjustedF + 1))
        return False, None

    def startCatchUpProcess(self, ledgerType: int, proof: ConsistencyProof):
        logger.debug("{} started catching up with consistency proof {}".
                     format(self, proof))
        self.recvdConsistencyProofs[ledgerType] = {}
        if proof is not None:
            if ledgerType == 0:
                self.mode = Mode.discovering
            elif ledgerType == 1:
                self.mode = Mode.syncing
            else:
                self.discard(proof, reason="Unknown ledger type {}".
                             format(ledgerType))
                return
            p = ConsistencyProof(*proof)
            rids = [self.nodestack.getRemote(nm).uid for nm in
                    self.nodestack.conns]
            for req in zip(self.getCatchupReqs(p), rids):
                self.send(*req)
            self.catchUpTill[getattr(p, f.LEDGER_TYPE.nm)] = p
        else:
            self.catchupCompleted(ledgerType)

    def catchupCompleted(self, ledgerType: int):
        logger.debug("{} completed catching up ledger {}".format(self,
                                                                 ledgerType))
        if ledgerType == 0:
            self.mode = Mode.discovered
            self.postPoolLedgerCaughtUp()
        elif ledgerType == 1:
            loop = asyncio.get_event_loop()
            runWithLoop(loop, self.postDomainLedgerCaughtUp)
            # TODO: This should be moved in `postDomainLedgerCaughtUp`
            self.mode = Mode.participating
        else:
            logger.error("{} called catchup completed for ledger {}".
                         format(self, ledgerType))
        # TODO: This should be moved under `if ledgerType == 0`
        self.checkInstances()

    def postPoolLedgerCaughtUp(self):
        # Node has discovered other nodes now sync up domain ledger
        for nm in self.nodestack.connecteds:
            self.sendDomainLedgerStatus(nm)
        self.processStashedLedgerStatuses()

    async def postDomainLedgerCaughtUp(self):
        """
        Process any stashed ordered requests and set the mode to
        `participating`
        :return:
        """
        logger.debug("{} before processing stashed ordered".format(self))
        await self.processStashedOrderedReqs()
        logger.debug("{} after processing stashed ordered".format(self))

    def getCatchupReqs(self, consProof: ConsistencyProof):
        nodeCount = len(self.nodestack.conns)
        start = getattr(consProof, f.SEQ_NO_START.nm)
        end = getattr(consProof, f.SEQ_NO_END.nm)
        batchLength = math.ceil((end-start)/nodeCount)
        reqs = []
        s = start + 1
        e = min(s + batchLength - 1, end)
        for i in range(nodeCount):
            reqs.append(CatchupReq(getattr(consProof, f.LEDGER_TYPE.nm),
                                   s, e))
            s = e + 1
            e = min(s + batchLength - 1, end)
            if s > end:
                break
        return reqs

    def _compareLedger(self, status: LedgerStatus):
        ledgerType = getattr(status, f.LEDGER_TYPE.nm)
        seqNo = getattr(status, f.TXN_SEQ_NO.nm)
        ledger = self.getLedgerForMsg(status)
        logger.debug(
            "{} comparing its ledger {} of size {} with {}".format(self,
                                                                   ledgerType,
                                                                   ledger.seqNo,
                                                                   seqNo))
        return ledger.seqNo - seqNo

    def isLedgerOld(self, status: LedgerStatus):
        return self._compareLedger(status) < 0

    def isLedgerNew(self, status: LedgerStatus):
        return self._compareLedger(status) > 0

    def isLedgerSame(self, status: LedgerStatus):
        return self._compareLedger(status) == 0

    def getLedgerForMsg(self, msg: Any):
        if self.isPoolLedgerMsg(msg):
            return self.poolManager.poolTxnStore
        elif self.isDomainLedgerMsg(msg):
            return self.primaryStorage
        else:
            self.discard(msg, reason="Invalid ledger msg type")

    def sendPoolLedgerStatus(self, nodeName):
        self.sendLedgerStatus(nodeName, 0)

    def sendDomainLedgerStatus(self, nodeName):
        self.sendLedgerStatus(nodeName, 1)

    def sendLedgerStatus(self, nodeName: str, ledgerType: int):
        rid = self.nodestack.getRemote(nodeName).uid
        ledgerStatus = self.poolLedgerStatus if ledgerType == 0 else \
            self.domainLedgerStatus
        if ledgerStatus:
            self.send(ledgerStatus, rid)
        else:
            logger.debug("{} not sending ledger {} status to {} as it is null"
                         .format(self, ledgerType, nodeName))

    @staticmethod
    def isPoolLedgerMsg(msg: Any):
        return getattr(msg, f.LEDGER_TYPE.nm) == 0

    @staticmethod
    def isDomainLedgerMsg(msg: Any):
        return getattr(msg, f.LEDGER_TYPE.nm) == 1

    def getConsistencyProof(self, status: LedgerStatus):
        ledger = self.getLedgerForMsg(status)    # type: Ledger
        seqNoStart = getattr(status, f.TXN_SEQ_NO.nm)
        seqNoEnd = ledger.size
        if seqNoStart == 0:
            # Consistency proof for an empty tree cannot exist. Using the root
            # hash now so that the node which is behind can verify that
            # TODO: Make this an empty list
            oldRoot = ledger.tree.root_hash
            proof = [oldRoot, ]
        else:
            proof = ledger.tree.consistency_proof(seqNoStart, seqNoEnd)
            oldRoot = ledger.tree.merkle_tree_hash(0, seqNoStart)
        newRoot = ledger.tree.merkle_tree_hash(0, seqNoEnd)
        return ConsistencyProof(
            getattr(status, f.LEDGER_TYPE.nm),
            seqNoStart,
            seqNoEnd,
            b64encode(oldRoot).decode(),
            b64encode(newRoot).decode(),
            [b64encode(p).decode() for p in
             proof]
        )

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

        reply = await self.getReplyFor(request)
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

        # # Only propagate if the node is participating in the consensus process
        # # which happens when the node has completed the catchup process
        self.propagate(request, clientName)
        self.tryForwarding(request)

    async def processOrdered(self, ordered: Ordered, retryNo: int = 0):
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
                await self.executeRequest(ppTime, req)
                logger.debug("Node {} executing client request {} {}".
                             format(self.name, identifier, reqId))
            # If the client request hasn't reached the node but corresponding
            # PROPAGATE, PRE-PREPARE, PREPARE and COMMIT request did,
            # then retry 3 times
            elif retryNo < 3:
                retryNo += 1
                asyncio.sleep(random.randint(2, 4))
                await self.processOrdered(ordered, retryNo)
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
            # Record instance changes for views but send instance change
            # only when found master to be degraded. if quorum of view changes
            #  found then change view even if master not degraded
            if not self.instanceChanges.hasInstChngFrom(instChg.viewNo, frm):
                self.instanceChanges.addVote(instChg.viewNo, frm)
            if self.monitor.isMasterDegraded():
                logger.debug(
                    "{} found master degraded after receiving instance change "
                    "message from {}".format(self, frm))
                self.sendInstanceChange(instChg.viewNo)
            else:
                logger.debug(
                    "{} received instance change message {} but did not "
                    "find the master to be slow".format(self, instChg))
            if self.canViewChange(instChg.viewNo):
                logger.debug("{} initiating a view change with view "
                             "no {}".format(self, self.viewNo))
                self.startViewChange(instChg.viewNo)
            else:
                logger.trace("{} cannot initiate a view change".format(self))

    def checkPerformance(self):
        """
        Check if master instance is slow and send an instance change request.
        :returns True if master performance is OK, otherwise False
        """
        logger.debug("{} checking its performance".format(self))
        self._schedule(self.checkPerformance, self.perfCheckFreq)

        # Move ahead only if the node has synchronized its state with other
        # nodes
        if not self.isParticipating:
            return

        if self.instances.masterId is not None:
            if self.monitor.isMasterDegraded():
                self.sendInstanceChange(self.viewNo)
                return False
            else:
                logger.debug("{}s master has higher performance than backups".
                             format(self))
        return True

    def sendInstanceChange(self, viewNo: int):
        """
        Broadcast an instance change request to all the remaining nodes

        :param viewNo: the view number when the instance change is requested
        """
        logger.info("{} master has lower performance than backups. "
                    "Sending an instance change with viewNo {}".
                    format(self, viewNo))
        logger.info("{} metrics for monitor: {}".
                    format(self, self.monitor.prettymetrics))
        self.send(InstanceChange(viewNo))
        self.instanceChanges.addVote(viewNo, self.name)

    @property
    def quorum(self) -> int:
        r"""
        Return the quorum of this RBFT system. Equal to :math:`2f + 1`.
        """
        return (2 * self.f) + 1

    def primaryFound(self):
        # If the node has primary replica of master instance
        self.monitor.hasMasterPrimary = self.primaryReplicaNo == 0

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
        logger.display("{} authenticated {} signature on {}request {}".
                     format(self, identifier, typ, req['reqId']),
                     extra={"cli": True})

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

        If a request makes it this far, the signature has been verified
        to match the identifier.
        """
        if request.operation.get(TXN_TYPE) in POOL_TXN_TYPES:
            self.poolManager.checkRequestAuthorized(request)

    async def executeRequest(self, ppTime: float, req: Request) -> None:
        """
        Execute the REQUEST sent to this Node

        :param viewNo: the view number (See glossary)
        :param ppTime: the time at which PRE-PREPARE was sent
        :param req: the client REQUEST
        """

        await self.requestExecuter[req.operation.get(TXN_TYPE)](ppTime, req)

    # TODO: Find a better name for the function
    async def doCustomAction(self, ppTime, req):
        reply = await self.generateReply(ppTime, req)
        merkleProof = await self.primaryStorage.append(
            identifier=req.identifier, reply=reply, txnId=reply.result[TXN_ID])
        reply.result.update(merkleProof)
        self.transmitToClient(reply, self.clientIdentifiers[req.identifier])

    @staticmethod
    def genTxnId(identifier, reqId):
        return sha256("{}{}".format(identifier, reqId).encode()).hexdigest()

    async def generateReply(self, ppTime: float, req: Request) -> Reply:
        """
        Return a new clientReply created using the viewNo, request and the
        computed txnId of the request

        :param ppTime: the time at which PRE-PREPARE was sent
        :param req: the REQUEST
        :return: a Reply generated from the request
        """
        logger.debug("{} replying request {}".format(self, req))
        txnId = self.genTxnId(req.identifier, req.reqId)
        result = {f.IDENTIFIER.nm: req.identifier,
                  f.REQ_ID.nm: req.reqId,
                  TXN_ID: txnId,
                  TXN_TIME: ppTime,
                  TXN_TYPE: req.operation.get(TXN_TYPE)}

        for processor in self.reqProcessors:
            result.update(processor.process(req))

        return Reply(result)

    def defaultAuthNr(self):
        return SimpleAuthNr()

    async def getReplyFor(self, request):
        result = await self.secondaryStorage.getReply(request.identifier,
                                                      request.reqId)
        return Reply(result) if result else None

    async def processStashedOrderedReqs(self):
        i = 0
        while self.stashedOrderedReqs:
            msg = self.stashedOrderedReqs.pop()
            if not self.gotInCatchupReplies(msg):
                await self.processOrdered(msg)
            i += 1
        logger.debug("{} processed {} stashed ordered requests".format(self, i))
        # Resetting monitor after executing all stashed requests so no view
        # change can be proposed
        self.monitor.reset()
        return i

    def processStashedLedgerStatuses(self):
        i = 0
        while self.stashedLedgerStatuses:
            msg, frm = self.stashedLedgerStatuses.pop()
            i += 1
            self.processLedgerStatus(msg, frm)
        logger.debug("{} processed {} stashed ordered requests".format(self, i))
        return i

    def gotInCatchupReplies(self, msg):
        key = (getattr(msg, f.IDENTIFIER.nm), getattr(msg, f.REQ_ID.nm))
        return key in self.reqsFromCatchupReplies

    def startKeySharing(self, timeout=60):
        """
        Start key sharing till the timeout is reached.
        Other nodes will be able to join this node till the timeout is reached.

        :param timeout: the time till which key sharing is active
        """
        if self.nodestack.isKeySharing:
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
            self.nodestack.maintainConnections(force=force)

    def stopKeySharing(self, timedOut=False):
        """
        Stop key sharing, i.e don't allow any more nodes to join this node.
        """
        if self.nodestack.isKeySharing:
            if timedOut:
                logger.info("{} key sharing timed out; was not able to "
                            "connect to {}".
                            format(self,
                                   ", ".join(
                                       self.nodestack.notConnectedNodes())),
                            extra={"cli": "WARNING"})
            else:
                logger.info("{} completed key sharing".format(self),
                            extra={"cli": "STATUS"})
            self.nodestack.keep.auto = AutoMode.never

    @staticmethod
    def ensureKeysAreSetup(name, baseDir):
        """
        Check whether the keys are setup in the local RAET keep.
        Raises RaetKeysNotFoundException if not found.
        """
        if not isLocalKeepSetup(name, baseDir):
            raise REx(REx.reason)

    def reportSuspiciousNodeEx(self, ex: SuspiciousNode):
        """
        Report suspicion on a node on the basis of an exception
        """
        self.reportSuspiciousNode(ex.node, ex.reason, ex.code, ex.offendingMsg)

    def reportSuspiciousNode(self,
                             nodeName: str,
                             reason=None,
                             code: int=None,
                             offendingMsg=None):
        """
        Report suspicion on a node and add it to this node's blacklist.

        :param nodeName: name of the node to report suspicion on
        :param reason: the reason for suspicion
        """
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
        """
        Report suspicion on a client and add it to this node's blacklist.

        :param clientName: name of the client to report suspicion on
        :param reason: the reason for suspicion
        """
        logger.warning("{} suspicion raised on client {} for {}; "
                       "doing nothing for now".
                       format(self, clientName, reason))
        self.blacklistClient(clientName)

    def isClientBlacklisted(self, clientName: str):
        """
        Check whether the given client is in this node's blacklist.

        :param clientName: the client to check for blacklisting
        :return: whether the client was blacklisted
        """
        return self.clientBlacklister.isBlacklisted(clientName)

    def blacklistClient(self, clientName: str, reason: str=None, code: int=None):
        """
        Add the client specified by `clientName` to this node's blacklist
        """
        msg = "{} blacklisting client {}".format(self, clientName)
        if reason:
            msg += " for reason {}".format(reason)
        logger.debug(msg)
        self.clientBlacklister.blacklist(clientName)

    def isNodeBlacklisted(self, nodeName: str) -> bool:
        """
        Check whether the given node is in this node's blacklist.

        :param nodeName: the node to check for blacklisting
        :return: whether the node was blacklisted
        """
        return self.nodeBlacklister.isBlacklisted(nodeName)

    def blacklistNode(self, nodeName: str, reason: str=None, code: int=None):
        """
        Add the node specified by `nodeName` to this node's blacklist
        """
        msg = "{} blacklisting node {}".format(self, nodeName)
        if reason:
            msg += " for reason {}".format(reason)
        if code:
            msg += " for code {}".format(code)
        logger.debug(msg)
        self.nodeBlacklister.blacklist(nodeName)

    def transmitToClient(self, msg: Any, remoteName: str):
        self.clientstack.transmitToClient(msg, remoteName)

    def send(self, msg: Any, *rids: Iterable[int], signer: Signer = None):
        self.nodestack.send(msg, *rids, signer=signer)

    def __enter__(self):
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def logstats(self):
        """
        Print the node's current statistics to log.
        """
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
                    format(time.perf_counter() - self.nodestack.nextCheck))
        l("node connections        : {}".format(self.nodestack.conns))
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

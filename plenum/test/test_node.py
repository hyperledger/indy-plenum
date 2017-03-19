import operator
import time
import types
from collections import OrderedDict
from contextlib import ExitStack
from copy import copy
from functools import partial
from itertools import combinations, permutations
from typing import Iterable, Iterator, Tuple, Sequence, Union, Dict, TypeVar, \
    List

import plenum.test.delayers as delayers
from plenum.common.error import error
from plenum.common.eventually import eventually, eventuallyAll
from plenum.common.exceptions import RemoteNotFound
from plenum.common.log import getlogger
from plenum.common.looper import Looper
from plenum.common.port_dispenser import genHa
from plenum.common.stacked import NodeStack, ClientStack, KITStack
from plenum.common.startable import Status
from plenum.common.types import TaggedTuples, NodeDetail, CLIENT_STACK_SUFFIX
from plenum.common.util import Seconds, getMaxFailures, adict
from plenum.persistence import orientdb_store
from plenum.server import replica
from plenum.server.instances import Instances
from plenum.server.monitor import Monitor
from plenum.server.node import Node
from plenum.server.primary_elector import PrimaryElector
from plenum.test.greek import genNodeNames
from plenum.test.msgs import TestMsg
from plenum.test.spy_helpers import getLastMsgReceivedForNode, \
    getAllMsgReceivedForNode, getAllArgs
from plenum.test.stasher import Stasher
from plenum.test.test_client import TestClient
from plenum.test.test_ledger_manager import TestLedgerManager
from plenum.test.test_stack import StackedTester, getTestableStack, CONNECTED, \
    checkRemoteExists, RemoteState, checkState
from plenum.test.testable import Spyable
from plenum.test.waits import expectedWait

logger = getlogger()


NodeRef = TypeVar('NodeRef', Node, str)


# noinspection PyUnresolvedReferences
# noinspection PyShadowingNames
class TestNodeCore(StackedTester):
    def __init__(self, *args, **kwargs):
        self.nodeMsgRouter.routes[TestMsg] = self.eatTestMsg
        self.nodeIbStasher = Stasher(self.nodeInBox,
                                     "nodeInBoxStasher~" + self.name)
        self.clientIbStasher = Stasher(self.clientInBox,
                                       "clientInBoxStasher~" + self.name)
        self.actionQueueStasher = Stasher(self.actionQueue,
                                          "actionQueueStasher~" + self.name)

        # alter whitelist to allow TestMsg type through without sig
        self.authnWhitelist = self.authnWhitelist + (TestMsg,)

        # Nodes that wont be blacklisted by this node if the suspicion code
        # is among the set of suspicion codes mapped to its name. If the set of
        # suspicion codes is empty then the node would not be blacklisted for
        #  any suspicion code
        self.whitelistedNodes = {}          # type: Dict[str, Set[int]]

        # Clients that wont be blacklisted by this node if the suspicion code
        # is among the set of suspicion codes mapped to its name. If the set of
        # suspicion codes is empty then the client would not be blacklisted for
        #  suspicion code
        self.whitelistedClients = {}          # type: Dict[str, Set[int]]

        # Reinitialize the monitor
        d, l, o = self.monitor.Delta, self.monitor.Lambda, self.monitor.Omega
        notifierEventTriggeringConfig = self.monitor.notifierEventTriggeringConfig
        self.instances = Instances()

        self.nodeInfo = {
            'data': {}
        }

        pluginPaths = kwargs.get('pluginPaths', [])
        self.monitor = TestMonitor(self.name, d, l, o, self.instances,
                                   MockedNodeStack(), MockedBlacklister(),
                                   nodeInfo=self.nodeInfo,
                                   notifierEventTriggeringConfig=notifierEventTriggeringConfig,
                                   pluginPaths=pluginPaths)
        for i in range(len(self.replicas)):
            self.monitor.addInstance()

    async def processNodeInBox(self):
        self.nodeIbStasher.process()
        await super().processNodeInBox()

    async def processClientInBox(self):
        self.clientIbStasher.process()
        await super().processClientInBox()

    def _serviceActions(self):
        self.actionQueueStasher.process()
        return super()._serviceActions()

    def createReplica(self, instNo: int, isMaster: bool):
        return TestReplica(self, instNo, isMaster)

    def newPrimaryDecider(self):
        pdCls = self.primaryDecider if self.primaryDecider else \
            TestPrimaryElector
        return pdCls(self)

    def delaySelfNomination(self, delay: Seconds):
        logger.debug("{} delaying start election".format(self))
        delayerElection = partial(delayers.delayerMethod,
                                  TestPrimaryElector.startElection)
        self.elector.actionQueueStasher.delay(delayerElection(delay))

    def delayCheckPerformance(self, delay: Seconds):
        logger.debug("{} delaying check performance".format(self))
        delayerCheckPerf = partial(delayers.delayerMethod,
                                   TestNode.checkPerformance)
        self.actionQueueStasher.delay(delayerCheckPerf(delay))

    def resetDelays(self):
        logger.debug("{} resetting delays".format(self))
        self.nodestack.resetDelays()
        self.nodeIbStasher.resetDelays()
        for r in self.replicas:
            r.outBoxTestStasher.resetDelays()

    def whitelistNode(self, nodeName: str, *codes: int):
        if nodeName not in self.whitelistedClients:
            self.whitelistedClients[nodeName] = set()
        self.whitelistedClients[nodeName].update(codes)
        logger.debug("{} white listing {} for codes {}"
                      .format(self, nodeName, codes))

    def blacklistNode(self, nodeName: str, reason: str=None, code: int=None):
        if nodeName in self.whitelistedClients:
            # If node whitelisted for all codes
            if len(self.whitelistedClients[nodeName]) == 0:
                return
            # If no code is provided or node is whitelisted for that code
            elif code is None or code in self.whitelistedClients[nodeName]:
                return
        super().blacklistNode(nodeName, reason, code)

    def whitelistClient(self, clientName: str, *codes: int):
        if clientName not in self.whitelistedClients:
            self.whitelistedClients[clientName] = set()
        self.whitelistedClients[clientName].update(codes)
        logger.debug("{} white listing {} for codes {}"
                      .format(self, clientName, codes))

    def blacklistClient(self, clientName: str, reason: str=None, code: int=None):
        if clientName in self.whitelistedClients:
            # If node whitelisted for all codes
            if len(self.whitelistedClients[clientName]) == 0:
                return
            # If no code is provided or node is whitelisted for that code
            elif code is None or code in self.whitelistedClients[clientName]:
                return
        super().blacklistClient(clientName, reason, code)

    def validateNodeMsg(self, wrappedMsg):
        nm = TestMsg.__name__
        if nm not in TaggedTuples:
            TaggedTuples[nm] = TestMsg
        return super().validateNodeMsg(wrappedMsg)

    async def eatTestMsg(self, msg, frm):
        logger.debug("{0} received Test message: {1} from {2}".
                      format(self.nodestack.name, msg, frm))

    def serviceReplicaOutBox(self, *args, **kwargs) -> int:
        for r in self.replicas:  # type: TestReplica
            r.outBoxTestStasher.process()
        return super().serviceReplicaOutBox(*args, **kwargs)

    @classmethod
    def ensureKeysAreSetup(cls, name, baseDir):
        pass


@Spyable(methods=[Node.handleOneNodeMsg,
                  Node.handleInvalidClientMsg,
                  Node.processRequest,
                  Node.processOrdered,
                  Node.postToClientInBox,
                  Node.postToNodeInBox,
                  "eatTestMsg",
                  Node.decidePrimaries,
                  Node.startViewChange,
                  Node.discard,
                  Node.reportSuspiciousNode,
                  Node.reportSuspiciousClient,
                  Node.processPropagate,
                  Node.propagate,
                  Node.forward,
                  Node.send,
                  Node.sendInstanceChange,
                  Node.processInstanceChange,
                  Node.checkPerformance
                  ])
class TestNode(TestNodeCore, Node):
    def __init__(self, *args, **kwargs):
        Node.__init__(self, *args, **kwargs)
        TestNodeCore.__init__(self, *args, **kwargs)
        # Balances of all client
        self.balances = {}  # type: Dict[str, int]

        # Txns of all clients, each txn is a tuple like (from, to, amount)
        self.txns = []  # type: List[Tuple]

    def _getOrientDbStore(self, name, dbType):
        return orientdb_store.createOrientDbInMemStore(
            self.config, name, dbType)

    @property
    def nodeStackClass(self) -> NodeStack:
        return getTestableStack(Spyable(methods=[KITStack.handleJoinFromUnregisteredRemote], deepLevel=3)(NodeStack))

    @property
    def clientStackClass(self) -> ClientStack:
        return getTestableStack(ClientStack)

    def getLedgerManager(self):
        return TestLedgerManager(self, ownedByNode=True)


class TestPrimaryElector(PrimaryElector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actionQueueStasher = Stasher(self.actionQueue,
                                          "actionQueueStasher~elector~" +
                                          self.name)

    def _serviceActions(self):
        self.actionQueueStasher.process()
        return super()._serviceActions()


@Spyable(methods=[replica.Replica.doPrePrepare,
                  replica.Replica.canProcessPrePrepare,
                  replica.Replica.canSendPrepare,
                  replica.Replica.isValidPrepare,
                  replica.Replica.addToPrePrepares,
                  replica.Replica.processPrePrepare,
                  replica.Replica.processPrepare,
                  replica.Replica.processCommit,
                  replica.Replica.doPrepare,
                  replica.Replica.doOrder,
                  replica.Replica.discard,
                  # replica.Replica.orderPendingCommit
                  ])
class TestReplica(replica.Replica):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Each TestReplica gets it's own outbox stasher, all of which TestNode
        # processes in its overridden serviceReplicaOutBox
        self.outBoxTestStasher = \
            Stasher(self.outBox, "replicaOutBoxTestStasher~" + self.name)


class TestNodeSet(ExitStack):

    def __init__(self,
                 names: Iterable[str] = None,
                 count: int = None,
                 nodeReg=None,
                 tmpdir=None,
                 keyshare=True,
                 primaryDecider=None,
                 pluginPaths:Iterable[str]=None,
                 testNodeClass=TestNode):
        super().__init__()
        self.tmpdir = tmpdir
        self.primaryDecider = primaryDecider
        self.pluginPaths = pluginPaths

        self.testNodeClass = testNodeClass
        self.nodes = OrderedDict()  # type: Dict[str, TestNode]
        # Can use just self.nodes rather than maintaining a separate dictionary
        # but then have to pluck attributes from the `self.nodes` so keeping
        # it simple a the cost of extra memory and its test code so not a big
        # deal
        if nodeReg:
            self.nodeReg = nodeReg
        else:
            nodeNames = (names if names is not None and count is None else
                         genNodeNames(count) if count is not None else
                         error("only one of either names or count is required"))
            self.nodeReg = genNodeReg(
                    names=nodeNames)  # type: Dict[str, NodeDetail]
        for name in self.nodeReg.keys():
            self.addNode(name)
        # The following lets us access the nodes by name as attributes of the
        # NodeSet. It's not a problem unless a node name shadows a member.
        self.__dict__.update(self.nodes)

    def addNode(self, name: str) -> TestNode:
        if name in self.nodes:
            error("{} already added".format(name))
        assert name in self.nodeReg
        ha, cliname, cliha = self.nodeReg[name]

        testNodeClass = self.testNodeClass
        node = self.enter_context(
                testNodeClass(name=name,
                              ha=ha,
                              cliname=cliname,
                              cliha=cliha,
                              nodeRegistry=copy(self.nodeReg),
                              basedirpath=self.tmpdir,
                              primaryDecider=self.primaryDecider,
                              pluginPaths=self.pluginPaths))
        self.nodes[name] = node
        self.__dict__[name] = node
        return node

    def removeNode(self, name, shouldClean):
        self.nodes[name].stop()
        if shouldClean:
            self.nodes[name].nodestack.keep.clearAllDir()
            self.nodes[name].clientstack.keep.clearAllDir()
        del self.nodes[name]
        del self.__dict__[name]
        # del self.nodeRegistry[name]
        # for node in self:
        #     node.removeNodeFromRegistry(name)

    def __iter__(self) -> Iterator[TestNode]:
        return self.nodes.values().__iter__()

    def __getitem__(self, key) -> TestNode:
        return self.nodes.get(key)

    def __len__(self):
        return self.nodes.__len__()

    @property
    def nodeNames(self):
        return sorted(self.nodes.keys())

    @property
    def f(self):
        return getMaxFailures(len(self.nodes))

    def getNode(self, node: NodeRef) -> TestNode:
        return node if isinstance(node, Node) \
            else self.nodes.get(node) if isinstance(node, str) \
            else error("Expected a node or node name")

    @staticmethod
    def getNodeName(node: NodeRef) -> str:
        return node if isinstance(node, str) \
            else node.name if isinstance(node, Node) \
            else error("Expected a node or node name")

    def connect(self, fromNode: NodeRef, toNode: NodeRef):
        fr = self.getNode(fromNode)
        to = self.getNode(toNode)
        fr.connect(to.nodestack.ha)

    def connectAll(self):
        for c in combinations(self.nodes.keys(), 2):
            print("connecting {} to {}".format(*c))
            self.connect(*c)

    def getLastMsgReceived(self, node: NodeRef, method: str = None) -> Tuple:
        return getLastMsgReceivedForNode(self.getNode(node), method)

    def getAllMsgReceived(self, node: NodeRef, method: str = None) -> Tuple:
        return getAllMsgReceivedForNode(self.getNode(node), method)


def getNonPrimaryReplicas(nodes: Iterable[TestNode], instId: int = 0) -> \
        Sequence[TestReplica]:
    return [node.replicas[instId] for node in nodes if
            node.replicas[instId].isPrimary is False]


def getAllReplicas(nodes: Iterable[TestNode], instId: int = 0) -> \
        Sequence[TestReplica]:
    return [node.replicas[instId] for node in nodes]


@Spyable(methods=[Monitor.isMasterThroughputTooLow,
                  Monitor.isMasterReqLatencyTooHigh,
                  Monitor.sendThroughput,
                  Monitor.requestOrdered,
                  Monitor.reset])
class TestMonitor(Monitor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.masterReqLatenciesTest = {}

    def requestOrdered(self, identifier: str, reqId: int, instId: int,
                       byMaster: bool = False):
        duration = super().requestOrdered(identifier, reqId, instId, byMaster)
        if byMaster and duration is not None:
            self.masterReqLatenciesTest[(identifier, reqId)] = duration

    def reset(self):
        super().reset()
        self.masterReqLatenciesTest = {}


class Pool:
    def __init__(self, tmpdir_factory, testNodeSetClass=TestNodeSet):
        self.tmpdir_factory = tmpdir_factory
        self.testNodeSetClass = testNodeSetClass

    def run(self, coro, nodecount=4):
        tmpdir = self.fresh_tdir()
        with self.testNodeSetClass(count=nodecount, tmpdir=tmpdir) as nodeset:
            with Looper(nodeset) as looper:
                for n in nodeset:
                    n.startKeySharing()
                ctx = adict(looper=looper, nodeset=nodeset, tmpdir=tmpdir)
                looper.run(checkNodesConnected(nodeset))
                ensureElectionsDone(looper=looper, nodes=nodeset, retryWait=1,
                                    timeout=30)
                looper.run(coro(ctx))

    def fresh_tdir(self):
        return self.tmpdir_factory.mktemp('').strpath


class MockedNodeStack:
    def remotesByConnected(self):
        return [], []


class MockedBlacklister:
    def isBlacklisted(self, remote):
        return True


def checkPoolReady(looper: Looper, nodes: Sequence[TestNode],
                   timeout: int = 20):
    looper.run(
            eventually(checkNodesAreReady, nodes, retryWait=.25,
                       timeout=timeout,
                       ratchetSteps=10))


async def checkNodesCanRespondToClients(nodes):
    """
    Needed to make sure the web server has started. If we
    were keeping a web server on every node, we would
    have some node status that would tell us if the
    server was running or not.
    """

    def x():
        assert all(node.webserver.status == Status.started for node in nodes)

    await eventually(x)


async def checkNodesConnected(stacks: Iterable[Union[TestNode, TestClient]],
                              expectedRemoteState=None,
                              overrideTimeout=None):
    expectedRemoteState = expectedRemoteState if expectedRemoteState else CONNECTED
    # run for how long we expect all of the connections to take
    wait = overrideTimeout if overrideTimeout else expectedWait(len(stacks))
    logger.debug("waiting for {} seconds to check connections...".format(wait))
    # verify every node can see every other as a remote
    funcs = [
        partial(checkRemoteExists, frm.nodestack, to.name, expectedRemoteState)
        for frm, to in permutations(stacks, 2)]
    await eventuallyAll(*funcs,
                        retryWait=.5,
                        totalTimeout=wait,
                        acceptableExceptions=[AssertionError, RemoteNotFound])


def checkNodeRemotes(node: TestNode, states: Dict[str, RemoteState]=None,
                     state: RemoteState = None):
    assert states or state, "either state or states is required"
    assert not (
        states and state), "only one of state or states should be provided, " \
                           "but not both"
    for remote in node.nodestack.remotes.values():
        try:
            s = states[remote.name] if states else state
            checkState(s, remote, "from: {}, to: {}".format(node, remote.name))
        except Exception as ex:
            logger.debug("state checking exception is {} and args are {}"
                          "".format(ex, ex.args))
            raise RuntimeError(
                    "Error with {} checking remote {} in {}".format(node.name,
                                                                    remote.name,
                                                                    states
                                                                    )) from ex


def checkIfSameReplicaIPrimary(looper: Looper,
                               replicas: Sequence[TestReplica] = None,
                               retryWait: float = 1,
                               timeout: float = 20):
    # One and only one primary should be found and every replica should agree
    # on same primary

    def checkElectionDone():
        unknowns = sum(1 for r in replicas if r.isPrimary is None)
        assert unknowns == 0, "election should be complete, but {} out of {} " \
                              "don't know who the primary is for " \
                              "protocol no {}".\
            format(unknowns, len(replicas), replicas[0].instId)

    def checkPrisAreOne():  # number of expected primaries
        pris = sum(1 for r in replicas if r.isPrimary)
        assert pris == 1, "Primary count should be 1, but was {} for " \
                          "protocol no {}".format(pris, replicas[0].instId)

    def checkPrisAreSame():
        pris = {r.primaryName for r in replicas}
        assert len(pris) == 1, "Primary should be same for all, but were {} " \
                               "for protocol no {}" \
            .format(pris, replicas[0].instId)
    looper.run(
        eventuallyAll(checkElectionDone, checkPrisAreOne, checkPrisAreSame,
                      retryWait=retryWait, totalTimeout=timeout))


def checkNodesAreReady(nodes: Sequence[TestNode]):
    for node in nodes:
        assert node.isReady()


async def checkNodesParticipating(nodes: Sequence[TestNode], timeout: int=None):
    if not timeout:
        timeout = .75 * len(nodes)

    def chk():
        for node in nodes:
            assert node.isParticipating

    await eventually(chk, retryWait=1, timeout=timeout)


def checkEveryProtocolInstanceHasOnlyOnePrimary(looper: Looper,
                                                nodes: Sequence[TestNode],
                                                retryWait: float = None,
                                                timeout: float = None):

    coro = eventually(instances, nodes, retryWait=retryWait, timeout=timeout)
    insts, timeConsumed = timeThis(looper.run, coro)

    # TODO refactor this to just user eventuallyAll
    newTimeout = timeout - timeConsumed if timeout is not None else None

    for instId, replicas in insts.items():
        logger.debug("Checking replicas in instance: {}".format(instId))
        checkIfSameReplicaIPrimary(looper=looper,
                                   replicas=replicas,
                                   retryWait=retryWait,
                                   timeout=newTimeout)


def checkEveryNodeHasAtMostOnePrimary(looper: Looper,
                                      nodes: Sequence[TestNode],
                                      retryWait: float = None,
                                      timeout: float = None):
    def checkAtMostOnePrim(node):
        prims = [r for r in node.replicas if r.isPrimary]
        assert len(prims) <= 1

    for node in nodes:
        looper.run(eventually(checkAtMostOnePrim,
                              node,
                              retryWait=retryWait,
                              timeout=timeout))


def checkProtocolInstanceSetup(looper: Looper, nodes: Sequence[TestNode],
                               retryWait: float = 1,
                               timeout: float = None):
    checkEveryProtocolInstanceHasOnlyOnePrimary(
        looper=looper, nodes=nodes, retryWait=retryWait,
        timeout=timeout if timeout else None)

    checkEveryNodeHasAtMostOnePrimary(looper=looper, nodes=nodes,
                                      retryWait=retryWait, timeout=timeout / 5)

    primaryReplicas = {replica.instId: replica
                       for node in nodes
                       for replica in node.replicas if replica.isPrimary}
    return [r[1] for r in
            sorted(primaryReplicas.items(), key=operator.itemgetter(0))]


def ensureElectionsDone(looper: Looper,
                        nodes: Sequence[TestNode],
                        retryWait: float = None,
                        timeout: float = None) -> Sequence[TestNode]:
    # Wait for elections to be complete and returns the primary replica for
    # each protocol instance

    checkPoolReady(looper=looper, nodes=nodes,
                   timeout=timeout / 3 if timeout else None)

    return checkProtocolInstanceSetup(
        looper=looper, nodes=nodes, retryWait=retryWait,
        timeout=2 * timeout / 3 if timeout else None)


def genNodeReg(count=None, names=None) -> Dict[str, NodeDetail]:
    """

    :param count: number of nodes, mutually exclusive with names
    :param names: iterable with names of nodes, mutually exclusive with count
    :return: dictionary of name: (node stack HA, client stack name, client stack HA)
    """
    if names is None:
        names = genNodeNames(count)
    nodeReg = OrderedDict(
            (n, NodeDetail(genHa(), n + CLIENT_STACK_SUFFIX, genHa())) for n in
            names)

    def extractCliNodeReg(self):
        return OrderedDict((n.cliname, n.cliha) for n in self.values())

    nodeReg.extractCliNodeReg = types.MethodType(extractCliNodeReg, nodeReg)
    return nodeReg


def prepareNodeSet(looper: Looper, nodeSet: TestNodeSet):
    # TODO: Come up with a more specific name for this

    for n in nodeSet:
        n.startKeySharing()

    # Key sharing party
    looper.run(checkNodesConnected(nodeSet))

    # Remove all the nodes
    for n in list(nodeSet.nodes.keys()):
        looper.removeProdable(nodeSet.nodes[n])
        nodeSet.removeNode(n, shouldClean=False)


def checkViewChangeInitiatedForNode(node: TestNode, proposedViewNo: int):
    """
    Check if view change initiated for a given node
    :param node: The node to check for
    :param proposedViewNo: The view no which is proposed
    :return:
    """
    params = [args for args in getAllArgs(node, TestNode.startViewChange)]
    assert len(params) > 0
    args = params[-1]
    assert args["proposedViewNo"] == proposedViewNo
    assert node.viewNo == proposedViewNo
    assert node.elector.viewNo == proposedViewNo


def timeThis(func, *args, **kwargs):
    s = time.perf_counter()
    res = func(*args, **kwargs)
    return res, time.perf_counter() - s


def instances(nodes: Sequence[Node]) -> Dict[int, List[replica.Replica]]:
    instCount = getRequiredInstances(len(nodes))
    for n in nodes:
        assert len(n.replicas) == instCount
    return {i: [n.replicas[i] for n in nodes]
            for i in range(instCount)}


def getRequiredInstances(nodeCount: int) -> int:
    f_value = getMaxFailures(nodeCount)
    return f_value + 1



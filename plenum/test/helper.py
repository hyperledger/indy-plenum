import inspect
import logging
import math
import operator
import random
import time
import types
from collections import OrderedDict
from contextlib import ExitStack
from copy import copy
from functools import partial
from itertools import combinations, permutations
from typing import TypeVar, Tuple, Iterable, Dict, Optional, NamedTuple, List, \
    Any, Sequence, Iterator
from typing import Union, Callable

from typing import Set

from plenum.client.signer import SimpleSigner
from plenum.common.exceptions import RemoteNotFound
from plenum.common.looper import Looper
from plenum.common.request_types import Request, TaggedTuple, OP_FIELD_NAME, \
    Reply, f, Ordered, PrePrepare, InstanceChange, TaggedTuples
from plenum.common.startable import Status
from plenum.common.txn import REPLY, REQACK
from plenum.common.util import randomString, error, getMaxFailures, \
    Seconds, adict
from raet.raeting import AutoMode, TrnsKind, PcktKind

from plenum.server.client_authn import SimpleAuthNr
from plenum.server.instances import Instances
from plenum.server.monitor import Monitor
from plenum.server.node import Node, CLIENT_STACK_SUFFIX, NodeDetail
from plenum.server.plugin_loader import PluginLoader
from plenum.server.primary_elector import PrimaryElector
from plenum.test.eventually import eventually, eventuallyAll
from plenum.test.greek import genNodeNames
from plenum.test.testing_utils import PortDispenser

from plenum.client.client import Client, ClientProvider
from plenum.common.stacked import NodeStacked, HA, Stack
from plenum.server import replica
from plenum.test.testable import Spyable, SpyableMethod

# checkDblImp()

DelayRef = NamedTuple("DelayRef", [
    ("op", Optional[str]),
    ("frm", Optional[str])])

RaetDelay = NamedTuple("RaetDelay", [
    ("tk", Optional[TrnsKind]),
    ("pk", Optional[PcktKind]),
    ("fromPort", Optional[int])])


class TestStack(Stack):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stasher = Stasher(self.rxMsgs,
                               "TestStack~" + self.name)

        self.delay = self.stasher.delay

    def _serviceStack(self, age):
        super()._serviceStack(age)
        self.stasher.process(age)

    def resetDelays(self):
        self.stasher.resetDelays()

    @staticmethod
    def _soft(func, *args):
        try:
            return func(*args)
        except ValueError:
            return None


class Stasher:
    def __init__(self, queue, name: str = None):
        self.delayRules = set()
        self.queue = queue
        self.delayeds = []
        self.created = time.perf_counter()
        self.name = name

    def delay(self, tester):
        """
        Delay messages for operation `op` when msg sent by node `frm`

        :param tester: a callable that takes as an argument the item
            from the queue and returns a number of seconds it should be delayed
        """
        self.delayRules.add(tester)

    def nodelay(self, tester):
        if tester in self.delayRules:
            self.delayRules.remove(tester)
        else:
            logging.debug("{} not present in {}".format(tester, self.name))

    def process(self, age: float = None):
        age = age if age is not None else time.perf_counter() - self.created
        self.stashAll(age)
        self.unstashAll(age)

    def stashAll(self, age):
        for tester in self.delayRules:
            for rx in list(self.queue):
                secondsToDelay = tester(rx)
                if secondsToDelay:
                    logging.debug("{} stashing message {} for {} seconds".
                                  format(self.name, rx, secondsToDelay))
                    self.delayeds.append((age + secondsToDelay, rx))
                    self.queue.remove(rx)

    def unstashAll(self, age):
        """
        Not terribly efficient, but for now, this is only used for testing.
        HasActionQueue is more efficient about knowing when to iterate through
        the delayeds.

        :param age: seconds since Stasher started
        """
        for d in self.delayeds:
            if age >= d[0]:
                logging.debug(
                        "{} unstashing message {} ({:.0f} milliseconds overdue)".
                            format(self.name, d[1], (age - d[0]) * 1000))
                self.queue.appendleft(d[1])
                self.delayeds.remove(d)

    def resetDelays(self):
        logging.debug("{} resetting delays".format(self.name))
        self.delayRules = set()


class NotConnectedToAny(Exception):
    pass


class NotFullyConnected(Exception):
    pass


# noinspection PyUnresolvedReferences
class StackedTester:
    def checkIfConnectedToAll(self):
        connected = 0
        # TODO refactor to not use values
        for address in self.nodeReg.values():
            for remote in self.nodestack.remotes.values():
                if HA(*remote.ha) == address:
                    if Stack.isRemoteConnected(remote):
                        connected += 1
                        break
        totalNodes = len(self.nodeReg)
        if connected == 0:
            raise NotConnectedToAny()
        elif connected < totalNodes:
            raise NotFullyConnected()
        else:
            assert connected == totalNodes

    async def ensureConnectedToNodes(self):
        wait = expectedWait(len(self.nodeReg))
        logging.debug(
                "waiting for {} seconds to check client connections to nodes...".format(
                        wait))
        await eventuallyAll(self.checkIfConnectedToAll, retryWait=.5,
                            totalTimeout=wait)


@Spyable(methods=[Client.handleOneNodeMsg])
class TestClient(Client, StackedTester):
    @staticmethod
    def stackType():
        return TestStack


@Spyable(methods=[Monitor.isMasterThroughputTooLow,
                    Monitor.isMasterReqLatencyTooHigh])
class TestMonitor(Monitor):
    pass


class TestPrimaryElector(PrimaryElector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actionQueueStasher = Stasher(self.actionQueue,
                                          "actionQueueStasher~elector~" + self.name)

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
                  replica.Replica.doPrepare])
class TestReplica(replica.Replica):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Each TestReplica gets it's own outbox stasher, all of which TestNode
        # processes in its overridden serviceReplicaOutBox
        self.outBoxTestStasher = \
            Stasher(self.outBox, "replicaOutBoxTestStasher~" + self.name)


# noinspection PyUnresolvedReferences
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
        self.whitelistedClients = {}          # type: Dict[str, Set[int]]

        # Clients that wont be blacklisted by this node if the suspicion code
        # is among the set of suspicion codes mapped to its name. If the set of
        # suspicion codes is empty then the client would not be blacklisted for
        #  suspicion code
        self.whitelistedClients = {}          # type: Dict[str, Set[int]]

        # Reinitialize the monitor
        d, l, o = self.monitor.Delta, self.monitor.Lambda, self.monitor.Omega
        self.instances = Instances()
        self.monitor = TestMonitor(self.name, d, l, o, self.instances)
        for i in range(len(self.replicas)):
            self.monitor.addInstance()

    @staticmethod
    def stackType():
        return TestStack

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
        logging.debug("{} delaying start election".format(self))
        delayerElection = partial(delayerMethod,
                                  TestPrimaryElector.startElection)
        self.elector.actionQueueStasher.delay(delayerElection(delay))

    def delayCheckPerformance(self, delay: Seconds):
        logging.debug("{} delaying check performance".format(self))
        delayerCheckPerf = partial(delayerMethod, TestNode.checkPerformance)
        self.actionQueueStasher.delay(delayerCheckPerf(delay))

    def resetDelays(self):
        logging.debug("{} resetting delays".format(self))
        self.nodestack.resetDelays()
        self.nodeIbStasher.resetDelays()
        for r in self.replicas:
            r.outBoxTestStasher.resetDelays()

    def whitelistNode(self, nodeName: str, *codes: int):
        if nodeName not in self.whitelistedClients:
            self.whitelistedClients[nodeName] = set()
        self.whitelistedClients[nodeName].update(codes)
        logging.debug("{} white listing {} for codes {}"
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
        logging.debug("{} white listing {} for codes {}"
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
        logging.debug("{0} received Test message: {1} from {2}".
                      format(self.nodestack.name, msg, frm))

    def serviceReplicaOutBox(self, *args, **kwargs) -> int:
        for r in self.replicas:  # type: TestReplica
            r.outBoxTestStasher.process()
        return super().serviceReplicaOutBox(*args, **kwargs)


# noinspection PyShadowingNames,PyShadowingNames
@Spyable(methods=[Node.handleOneNodeMsg,
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
                  Node.processRequest,
                  Node.processPropagate,
                  Node.propagate,
                  Node.forward,
                  Node.send,
                  Node.processInstanceChange,
                  Node.checkPerformance])
class TestNode(TestNodeCore, Node):
    def __init__(self, *args, **kwargs):
        Node.__init__(self, *args, **kwargs)
        TestNodeCore.__init__(self)


def randomMsg() -> TaggedTuple:
    return TestMsg('subject ' + randomString(),
                   'content ' + randomString())


NodeRef = TypeVar('NodeRef', Node, str)

RemoteState = NamedTuple("RemoteState", [
    ('joined', Optional[bool]),
    ('allowed', Optional[bool]),
    ('alived', Optional[bool])])


def ordinal(n):
    return "%d%s" % (
        n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])


def getLastMsgReceivedForNode(node: TestNode, method: str = None) -> Tuple:
    return node.spylog.getLast(
            method if method else NodeStacked.handleOneNodeMsg.__name__,
            required=True).params[
        'wrappedMsg']  # params should return a one element tuple


def getAllMsgReceivedForNode(node: TestNode, method: str = None) -> List:
    return [m.params['msg'] for m in
            node.spylog.getAll(method if method else "eatTestMsg")]


def getLastClientReqReceivedForNode(node: TestNode) -> Optional[Request]:
    requestEntry = node.spylog.getLast(Node.processRequest.__name__)
    # params should return a one element tuple
    return requestEntry.params['request'] if requestEntry else None


def getAllArgs(obj: Any, method: Union[str, Callable]) -> List[Any]:
    # params should return a List
    methodName = method if isinstance(method, str) else getCallableName(method)
    return [m.params for m in obj.spylog.getAll(methodName)]


def getAllReturnVals(obj: Any, method: SpyableMethod) -> List[Any]:
    # params should return a List
    methodName = method if isinstance(method, str) else getCallableName(method)
    return [m.result for m in obj.spylog.getAll(methodName)]


class TestNodeSet(ExitStack):
    def __init__(self,
                 names: Iterable[str] = None,
                 count: int = None,
                 nodeReg=None,
                 tmpdir=None,
                 keyshare=True,
                 primaryDecider=None,
                 opVerificationPluginPath=None,
                 testNodeClass=TestNode):
        super().__init__()

        self.tmpdir = tmpdir

        self.primaryDecider = primaryDecider
        self.opVerificationPluginPath = opVerificationPluginPath

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

        if self.opVerificationPluginPath:
            pl = PluginLoader(self.opVerificationPluginPath)
            opVerifiers = pl.plugins['VERIFICATION']
        else:
            opVerifiers = None

        testNodeClass = self.testNodeClass
        node = self.enter_context(
                testNodeClass(name=name,
                         ha=ha,
                         cliname=cliname,
                         cliha=cliha,
                         nodeRegistry=copy(self.nodeReg),
                         basedirpath=self.tmpdir,
                         primaryDecider=self.primaryDecider,
                         opVerifiers=opVerifiers))
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

# TODO: This file is becoming too big. Move things out of here.
# Start with TestNodeSet


def checkSufficientRepliesRecvd(receivedMsgs: Iterable, reqId: int,
                                fValue: int):
    receivedReplies = getRepliesFromClientInbox(receivedMsgs, reqId)
    logging.debug("received replies {}".format(receivedReplies))
    assert len(receivedReplies) > fValue
    result = None
    for reply in receivedReplies:
        if result is None:
            result = reply["result"]
        else:
            # all replies should have the same result
            assert reply["result"] == result

    assert all([r['reqId'] == reqId for r in receivedReplies])
    return result
    # TODO add test case for what happens when replies don't have the same data


def sendReqsToNodesAndVerifySuffReplies(looper: Looper, client: TestClient,
                                        numReqs: int, fVal: int=None,
                                        timeout: float=None):
    nodeCount = len(client.nodeReg)
    fVal = fVal or getMaxFailures(nodeCount)
    timeout = timeout or 3 * nodeCount

    requests = sendRandomRequests(client, numReqs)
    for request in requests:
        looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                              request.reqId, fVal,
                              retryWait=1, timeout=timeout))
    return requests


# noinspection PyIncorrectDocstring
def checkResponseCorrectnessFromNodes(receivedMsgs: Iterable, reqId: int,
                                      fValue: int) -> bool:
    """
    the client must get at least :math:`2f+1` responses
    """
    msgs = [(msg['reqId'], msg['result']['txnId']) for msg in
            getRepliesFromClientInbox(receivedMsgs, reqId)]
    groupedMsgs = {}
    # for (rid, tid, oprType, oprAmt) in msgs:
    for tpl in msgs:
        groupedMsgs[tpl] = groupedMsgs.get(tpl, 0) + 1
    assert max(groupedMsgs.values()) >= fValue + 1


def getRepliesFromClientInbox(inbox, reqId) -> list:
    return list({_: msg for msg, _ in inbox if
                 msg[OP_FIELD_NAME] == REPLY and msg[
                     f.REQ_ID.nm] == reqId}.values())


def checkLastClientReqForNode(node: TestNode, expectedRequest: Request):
    recvRequest = getLastClientReqReceivedForNode(node)
    assert recvRequest
    assert expectedRequest.__dict__ == recvRequest.__dict__


# noinspection PyIncorrectDocstring
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


def expectedWaitDirect(count):
    return count * 0.75 + 1


def expectedWait(nodeCount):
    c = totalConnections(nodeCount)
    w = expectedWaitDirect(c)
    logging.debug("wait time for {} nodes and {} connections is {}".format(
            nodeCount, c, w))
    return w


async def checkNodesConnected(stacks: Iterable[NodeStacked],
                              expectedRemoteState=None,
                              overrideTimeout=None):
    expectedRemoteState = expectedRemoteState if expectedRemoteState else CONNECTED
    # run for how long we expect all of the connections to take
    wait = overrideTimeout if overrideTimeout else expectedWait(len(stacks))
    logging.debug("waiting for {} seconds to check connections...".format(wait))
    # verify every node can see every other as a remote
    funcs = [
        partial(checkRemoteExists, frm.nodestack, to.name, expectedRemoteState)
        for frm, to in permutations(stacks, 2)]
    await eventuallyAll(*funcs,
                        retryWait=.5,
                        totalTimeout=wait,
                        acceptableExceptions=[AssertionError, RemoteNotFound])


def checkNodeRemotes(node: TestNode, states: Dict[str, RemoteState] = None,
                     state: RemoteState = None):
    assert states or state, "either state or states is required"
    assert not (
        states and state), "only one of state or states should be provided, but not both"
    for remote in node.nodestack.remotes.values():
        try:
            s = states[remote.name] if states else state
            checkState(s, remote, "from: {}, to: {}".format(node, remote.name))
        except Exception as ex:
            logging.debug("state checking exception is {} and args are {}"
                          "".format(ex, ex.args))
            raise RuntimeError(
                    "Error with {} checking remote {} in {}".format(node.name,
                                                                    remote.name,
                                                                    states
                                                                    )) from ex


# noinspection PyProtectedMember
def checkState(state: RemoteState, obj: Any, details: str=None):
    if state is not None:
        checkedItems = {}
        for key, s in state._asdict().items():
            checkedItems[key] = 'N/A' if s == 'N/A' else getattr(obj, key)
        actualState = RemoteState(**checkedItems)
        assert actualState == state


def checkRemoteExists(frm: Stack,
                      to: str,  # remoteName
                      state: Optional[RemoteState] = None):
    remote = frm.getRemote(to)
    checkState(state, remote, "{}'s remote {}".format(frm.name, to))


def checkPoolReady(looper: Looper, nodes: Sequence[TestNode],
                   timeout: int = 20):
    looper.run(
            eventually(checkNodesAreReady, nodes, retryWait=.25,
                       timeout=timeout,
                       ratchetSteps=10))


def checkIfSameReplicaIPrimary(looper: Looper,
                               replicas: Sequence[TestReplica] = None,
                               retryWait: float = 1,
                               timeout: float = 20):
    # One and only one primary should be found and every replica should agree
    # on same primary

    def checkElectionDone():
        unknowns = sum(1 for r in replicas if r.isPrimary is None)
        assert unknowns == 0, "election should be complete, but {} out of {} " \
                              "don't know who the primary is for protocol no {}" \
            .format(unknowns, len(replicas), replicas[0].instId)

    def checkPrisAreOne():  # number of expected primaries
        pris = sum(1 for r in replicas if r.isPrimary)
        assert pris == 1, "Primary count should be 1, but was {} for protocol no {}" \
            .format(pris, replicas[0].instId)

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


def instances(nodes: Sequence[Node]) -> Dict[int, List[replica.Replica]]:
    instCount = getRequiredInstances(len(nodes))
    for n in nodes:
        assert len(n.replicas) == instCount
    return {i: [n.replicas[i] for n in nodes]
            for i in range(instCount)}


def getRequiredInstances(nodeCount: int) -> int:
    f_value = getMaxFailures(nodeCount)
    return f_value + 1


def timeThis(func, *args, **kwargs):
    s = time.perf_counter()
    res = func(*args, **kwargs)
    return res, time.perf_counter() - s


def checkEveryProtocolInstanceHasOnlyOnePrimary(looper: Looper,
                                                nodes: Sequence[TestNode],
                                                retryWait: float = None,
                                                timeout: float = None):

    coro = eventually(instances, nodes, retryWait=retryWait, timeout=timeout)
    insts, timeConsumed = timeThis(looper.run, coro)

    # TODO refactor this to just user eventuallyAll
    newTimeout = timeout - timeConsumed if timeout is not None else None

    for instId, replicas in insts.items():
        logging.debug("Checking replicas in instance: {}".format(instId))
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


def getPendingRequestsForReplica(replica: TestReplica, requestType: Any):
    return [item[0] for item in replica.postElectionMsgs if
            isinstance(item[0], requestType)]


def assertLength(collection: Sequence[Any], expectedLength: int):
    assert len(
            collection) == expectedLength, "Observed length was {} but expected length was {}".format(
            len(collection), expectedLength)


def assertNonEmpty(elem: Any):
    assert elem is None


def checkNodesReadyForRequest(looper: Looper, nodes: Sequence[TestNode],
                              timeout: int = 20):
    checkPoolReady(looper, nodes, timeout)
    # checkNodesCanRespondToClients(nodes)


def setupNodesAndClient(looper: Looper, nodes: Sequence[TestNode], nodeReg=None,
                        tmpdir=None):
    looper.run(checkNodesConnected(nodes))
    timeout = 15 + 2 * (len(nodes))
    ensureElectionsDone(looper=looper, nodes=nodes, retryWait=1,
                        timeout=timeout)
    return setupClient(looper, nodes, nodeReg=nodeReg, tmpdir=tmpdir)


def setupClient(looper: Looper,
                nodes: Sequence[TestNode] = None,
                nodeReg=None,
                tmpdir=None):
    client1 = genTestClient(nodes=nodes,
                            nodeReg=nodeReg,
                            tmpdir=tmpdir)
    looper.add(client1)
    looper.run(client1.ensureConnectedToNodes())
    return client1


# noinspection PyIncorrectDocstring
async def aSetupClient(looper: Looper,
                       nodes: Sequence[TestNode] = None,
                       nodeReg=None,
                       tmpdir=None):
    """
    async version of above
    """
    client1 = genTestClient(nodes=nodes,
                            nodeReg=nodeReg,
                            tmpdir=tmpdir)
    looper.add(client1)
    await client1.ensureConnectedToNodes()
    return client1


def setupNodesAndClientAndSendRandomReq(looper: Looper,
                                        nodes: Sequence[TestNode], nodeReg=None,
                                        tmpdir=None):
    _client = setupNodesAndClient(looper, nodes, nodeReg, tmpdir)
    request = sendRandomRequest(_client)
    timeout = 3 * len(nodes)
    looper.run(eventually(checkSufficientRepliesRecvd,
                          _client.inBox,
                          request.reqId, 1,
                          retryWait=1, timeout=timeout))
    return _client, request


def getPrimaryReplica(nodes: Sequence[TestNode],
                      instId: int = 0) -> TestReplica:
    preplicas = [node.replicas[instId] for node in nodes if
                 node.replicas[instId].isPrimary]
    if len(preplicas) > 1:
        raise RuntimeError('More than one primary node found')
    elif len(preplicas) < 1:
        raise RuntimeError('No primary node found')
    else:
        return preplicas[0]


def getNonPrimaryReplicas(nodes: Iterable[TestNode], instId: int = 0) -> \
        Sequence[TestReplica]:
    return [node.replicas[instId] for node in nodes if
            node.replicas[instId].isPrimary is False]


def getAllReplicas(nodes: Iterable[TestNode], instId: int = 0) -> \
        Sequence[TestReplica]:
    return [node.replicas[instId] for node in nodes]


genHa = PortDispenser("127.0.0.1").getNext
# genHa = HaGen().getNext


def genTestClient(nodes: TestNodeSet = None,
                  nodeReg=None,
                  tmpdir=None,
                  signer=None,
                  testClientClass=TestClient,
                  bootstrapKeys=True) -> TestClient:
    nReg = nodeReg
    if nodeReg:
        assert isinstance(nodeReg, dict)
    elif hasattr(nodes, "nodeReg"):
        nReg = nodes.nodeReg.extractCliNodeReg()
    else:
        error("need access to nodeReg")

    for k, v in nReg.items():
        assert type(k) == str
        assert type(v) == HA

    ha = genHa()
    identifier = "testClient{}".format(ha.port)

    signer = signer if signer else SimpleSigner(identifier)

    tc = testClientClass(identifier,
                         nodeReg=nReg,
                         ha=ha,
                         basedirpath=tmpdir,
                         signer=signer)
    if bootstrapKeys and nodes:
        bootstrapClientKeys(tc, nodes)
    return tc


def bootstrapClientKeys(client, nodes):
    # bootstrap client verification key to all nodes
    for n in nodes:
        sig = client.getSigner()
        idAndKey = sig.identifier, sig.verkey
        n.clientAuthNr.addClient(*idAndKey)


def genTestClientProvider(nodes: TestNodeSet = None,
                          nodeReg=None,
                          tmpdir=None,
                          clientGnr=genTestClient):
    clbk = partial(clientGnr, nodes, nodeReg, tmpdir)
    return ClientProvider(clbk)


def totalConnections(nodeCount: int) -> int:
    return math.ceil((nodeCount * (nodeCount - 1)) / 2)


def randomOperation():
    return {"type": "buy", "amount": random.randint(10, 100)}


def sendRandomRequest(client: Client):
    return client.submit(randomOperation())[0]


def sendRandomRequests(client: Client, count: int):
    ops = [randomOperation() for _ in range(count)]
    return client.submit(*ops)


def buildCompletedTxnFromReply(request, reply: Reply) -> Dict:
    txn = request.operation
    txn.update(reply)
    return txn


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


CONNECTED = RemoteState(joined=True, allowed=True, alived=True)
NOT_CONNECTED = RemoteState(joined=None, allowed=None, alived=None)
JOINED_NOT_ALLOWED = RemoteState(joined=True, allowed=None, alived=None)
JOINED = RemoteState(joined=True, allowed='N/A', alived='N/A')


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


async def msgAll(nodes: TestNodeSet):
    # test sending messages from every node to every other node
    # TODO split send and check so that the messages can be sent concurrently
    for p in permutations(nodes.nodeNames, 2):
        await sendMsgAndCheck(nodes, p[0], p[1], timeout=3)


async def sendMsgAndCheck(nodes: TestNodeSet,
                          frm: NodeRef,
                          to: NodeRef,
                          msg: Optional[Tuple]=None,
                          timeout: Optional[int]=15
                          ):
    logging.debug("Sending msg from {} to {}".format(frm, to))
    msg = msg if msg else randomMsg()
    frmnode = nodes.getNode(frm)
    rid = frmnode.nodestack.getRemote(nodes.getNodeName(to)).uid
    frmnode.send(msg, rid)
    await eventually(checkMsg, msg, nodes, to, retryWait=.1, timeout=timeout,
                     ratchetSteps=10)


def checkMsg(msg, nodes, to, method: str = None):
    allMsgs = nodes.getAllMsgReceived(to, method)
    assert msg in allMsgs


def addNodeBack(nodeSet: TestNodeSet,
                looper: Looper,
                nodeName: str) -> TestNode:
    node = nodeSet.addNode(nodeName)
    looper.add(node)
    return node


def checkMethodCalled(node: TestNode,
                      method: str,
                      args: Tuple):
    assert node.spylog.getLastParams(method) == args


def delayer(seconds, op, senderFilter=None, instFilter: int = None):
    def inner(rx):
        msg, frm = rx
        if msg[OP_FIELD_NAME] == op and \
                (not senderFilter or frm == senderFilter) and \
                (instFilter is None or (f.INST_ID.nm in msg and msg[
                    f.INST_ID.nm] == instFilter)):
            return seconds

    return inner


def delayerMsgTuple(seconds, opType, senderFilter=None, instFilter: int = None):
    """
    Used for nodeInBoxStasher

    :param seconds:
    :param opType:
    :param senderFilter:
    :param instFilter:
    :return:
    """

    def inner(wrappedMsg):
        msg, frm = wrappedMsg
        if isinstance(msg, opType) and \
                (not senderFilter or frm == senderFilter) and \
                (instFilter is None or
                     (f.INST_ID.nm in msg._fields and
                              getattr(msg, f.INST_ID.nm) == instFilter)):
            return seconds

    return inner


def delayerMsgDict(seconds, op, instFilter: int = None):
    def inner(msg):
        if op == msg[OP_FIELD_NAME] and \
                (instFilter is None or (f.INST_ID.nm in msg and msg[
                    f.INST_ID.nm] == instFilter)):
            return seconds

    return inner


def delayerMethod(method, delay):
    def inner(action_pair):
        action, actionId = action_pair
        actionName = getCallableName(action)
        if actionName == method.__name__:
            return delay

    return inner


class Pool:
    def __init__(self, tmpdir_factory, counter, testNodeSetClass=TestNodeSet):
        self.tmpdir_factory = tmpdir_factory
        self.counter = counter
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
        return self.tmpdir_factory.getbasetemp().strpath + \
               '/' + str(next(self.counter))


def checkPropagateReqCountOfNode(node: TestNode, identifier: str, reqId: int):
    key = identifier, reqId
    assert key in node.requests
    assert len(node.requests[key].propagates) >= node.f + 1


def checkRequestReturnedToNode(node: TestNode, identifier: str, reqId: int,
                               digest: str, instId: int):
    params = getAllArgs(node, node.processOrdered)
    # Skipping the view no from each ordered request
    recvdOrderedReqs = [p['ordered'][:1] + p['ordered'][2:] for p in params]
    expected = (instId, identifier, reqId, digest)
    assert expected in recvdOrderedReqs


def checkPrePrepareReqSent(replica: TestReplica, req: Request):
    prePreparesSent = getAllArgs(replica, replica.doPrePrepare)
    expected = req.reqDigest
    assert expected in [p["reqDigest"] for p in prePreparesSent]


def checkPrePrepareReqRecvd(replicas: Iterable[TestReplica],
                            expectedRequest: PrePrepare):
    for replica in replicas:
        params = getAllArgs(replica, replica.canProcessPrePrepare)
        assert expectedRequest in [p['pp'] for p in params]


def checkPrepareReqSent(replica: TestReplica, identifier: str, reqId: int):
    paramsList = getAllArgs(replica, replica.canSendPrepare)
    rv = getAllReturnVals(replica,
                          replica.canSendPrepare)
    for params in paramsList:
        req = params['request']
        assert req.identifier == identifier
        assert req.reqId == reqId
    assert all(rv)


def checkSufficientPrepareReqRecvd(replica: TestReplica, viewNo: int,
                                   ppSeqNo: int):
    key = (viewNo, ppSeqNo)
    assert key in replica.prepares
    assert len(replica.prepares[key][1]) >= 2 * replica.f


def checkSufficientCommitReqRecvd(replicas: Iterable[TestReplica], viewNo: int,
                                  ppSeqNo: int):
    for replica in replicas:
        key = (viewNo, ppSeqNo)
        assert key in replica.commits
        received = len(replica.commits[key][1])
        minimum = 2 * replica.f
        assert received > minimum


def checkReqAck(client, node, reqId, update: Dict[str, str]=None):
    rec = {OP_FIELD_NAME: REQACK, 'reqId': reqId}
    if update:
        rec.update(update)
    expected = (rec, node.clientstack.name)
    # one and only one matching message should be in the client's inBox
    assert sum(1 for x in client.inBox if x == expected) == 1


def checkViewNoForNodes(nodes: Iterable[TestNode], expectedViewNo: int = None):
    """
    Checks if all the given nodes have the expected view no
    :param nodes: The nodes to check for
    :param expectedViewNo: the view no that the nodes are expected to have
    :return:
    """
    viewNos = set()
    for node in nodes:
        logging.debug("{}'s view no is {}".format(node, node.viewNo))
        viewNos.add(node.viewNo)
    assert len(viewNos) == 1
    vNo, = viewNos
    if expectedViewNo:
        assert vNo == expectedViewNo
    return vNo


def checkViewChangeInitiatedForNode(node: TestNode, oldViewNo: int):
    """
    Check if view change initiated for a given node
    :param node: The node to check for
    :param oldViewNo: The view no on which the nodes were before the view change
    :return:
    """
    params = [args for args in getAllArgs(
            node, TestNode.startViewChange)]
    assert len(params) > 0
    args = params[-1]
    assert args["proposedViewNo"] == oldViewNo
    assert node.viewNo == oldViewNo + 1
    assert node.elector.viewNo == oldViewNo + 1


# Delayer of PRE-PREPARE requests from a particular instance
def ppDelay(delay: float, instId: int=None):
    return delayerMsgTuple(delay, PrePrepare, instFilter=instId)


# Delayer of INSTANCE-CHANGE requests
def icDelay(delay: float):
    return delayerMsgTuple(delay, InstanceChange)


def delay(what, frm, to, howlong):
    if not isinstance(frm, Iterable):
        frm = [frm]
    if not isinstance(to, Iterable):
        to = [to]
    for f in frm:
        for t in to:
            if isinstance(t, TestNode):
                if isinstance(f, TestNode):
                    stasher = t.nodeIbStasher
                elif isinstance(f, TestClient):
                    stasher = t.clientIbStasher
                else:
                    raise TypeError(
                            "from type {} for {} not supported".format(type(f),
                                                                       f))
                stasher.delay(delayerMsgTuple(howlong, what, f.name))
            else:
                raise TypeError(
                        "to type {} for {} not supported".format(type(t), t))


def getNodeSuspicions(node: TestNode, code: int = None):
    params = getAllArgs(node, TestNode.reportSuspiciousNode)
    if params and code is not None:
        params = [param for param in params
                  if 'code' in param and param['code'] == code]
    return params


def getCallableName(callable: Callable):
    # If it is a function or method then access its `__name__`
    if inspect.isfunction(callable) or inspect.ismethod(callable):
        if hasattr(callable, "__name__"):
            return callable.__name__
        # If it is a partial then access its `func`'s `__name__`
        elif hasattr(callable, "func"):
            return callable.func.__name__
        else:
            RuntimeError("Do not know how to get name of this callable")
    else:
        TypeError("This is not a callable")


def checkDiscardMsg(nodeSet, discardedMsg,
                    reasonRegexp, *exclude):
    for n in filterNodeSet(nodeSet, exclude):
        last = n.spylog.getLastParams(Node.discard, required=True)
        assert last['msg'] == discardedMsg
        assert reasonRegexp in last['reason']


def filterNodeSet(nodeSet, exclude: List[Union[str, Node]]):
    """
    Return a set of nodes with the nodes in exclude removed.

    :param nodeSet: the set of nodes
    :param exclude: the list of nodes or node names to exclude
    :return: the filtered nodeSet
    """
    return [n for n in nodeSet
            if n not in
            [nodeSet[x] if isinstance(x, str) else x for x in exclude]]


def whitelistNode(toWhitelist: str, frm: Sequence[TestNode], *codes):
    for node in frm:
        node.whitelistNode(toWhitelist, *codes)


def whitelistClient(toWhitelist: str, frm: Sequence[TestNode], *codes):
    for node in frm:
        node.whitelistClient(toWhitelist, *codes)


TESTMSG = "TESTMSG"
TestMsg = TaggedTuple(TESTMSG, [
    ("subject", str),
    ("content", str)])
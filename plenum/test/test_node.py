import operator
import time
import types
from collections import OrderedDict
from contextlib import ExitStack
from copy import copy
from functools import partial
from itertools import combinations, permutations
from typing import Iterable, Iterator, Tuple, Sequence, Union, Dict, TypeVar, \
    List, Optional

from crypto.bls.bls_bft import BlsBft
from plenum.common.stacks import nodeStackClass, clientStackClass
from plenum.server.domain_req_handler import DomainRequestHandler
from stp_core.crypto.util import randomSeed
from stp_core.network.port_dispenser import genHa

import plenum.test.delayers as delayers
from plenum.common.error import error
from stp_core.loop.eventually import eventually, eventuallyAll
from stp_core.network.exceptions import RemoteNotFound
from plenum.common.keygen_utils import learnKeysFromOthers, tellKeysToOthers
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.common.startable import Status
from plenum.common.types import NodeDetail, f
from plenum.common.constants import CLIENT_STACK_SUFFIX, TXN_TYPE, \
    DOMAIN_LEDGER_ID, NYM, STATE_PROOF
from plenum.common.util import Seconds, getMaxFailures
from stp_core.common.util import adict
from plenum.server import replica
from plenum.server.instances import Instances
from plenum.server.monitor import Monitor
from plenum.server.node import Node
from plenum.server.primary_elector import PrimaryElector
from plenum.server.primary_selector import PrimarySelector
from plenum.test.greek import genNodeNames
from plenum.test.msgs import TestMsg
from plenum.test.spy_helpers import getLastMsgReceivedForNode, \
    getAllMsgReceivedForNode, getAllArgs
from plenum.test.stasher import Stasher
from plenum.test.test_client import TestClient
from plenum.test.test_ledger_manager import TestLedgerManager
from plenum.test.test_stack import StackedTester, getTestableStack, CONNECTED, \
    checkRemoteExists, RemoteState, checkState
from plenum.test.testable import spyable
from plenum.test import waits
from plenum.common.messages.node_message_factory import node_message_factory
from plenum.server.replicas import Replicas
from hashlib import sha256
from plenum.common.messages.node_messages import Reply

logger = getlogger()


class TestDomainRequestHandler(DomainRequestHandler):

    @staticmethod
    def prepare_buy_for_state(txn):
        from common.serializers.serialization import domain_state_serializer
        identifier = txn.get(f.IDENTIFIER.nm)
        value = domain_state_serializer.serialize({"amount": txn['amount']})
        key = TestDomainRequestHandler.prepare_buy_key(identifier)
        return key, value

    @staticmethod
    def prepare_buy_key(identifier):
        return sha256('{}:buy'.format(identifier).encode()).digest()

    def _updateStateWithSingleTxn(self, txn, isCommitted=False):
        typ = txn.get(TXN_TYPE)
        if typ == 'buy':
            key, value = self.prepare_buy_for_state(txn)
            self.state.set(key, value)
            logger.trace('{} after adding to state, headhash is {}'.
                         format(self, self.state.headHash))
        else:
            super()._updateStateWithSingleTxn(txn, isCommitted=isCommitted)

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
        self.monitor = TestMonitor(
            self.name,
            d,
            l,
            o,
            self.instances,
            MockedNodeStack(),
            MockedBlacklister(),
            nodeInfo=self.nodeInfo,
            notifierEventTriggeringConfig=notifierEventTriggeringConfig,
            pluginPaths=pluginPaths)
        for i in range(len(self.replicas)):
            self.monitor.addInstance()
        self.replicas._monitor = self.monitor

    def create_replicas(self):
        return TestReplicas(self, self.monitor)

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
            TestPrimarySelector
        return pdCls(self)

    def delaySelfNomination(self, delay: Seconds):
        if isinstance(self.primaryDecider, PrimaryElector):
            logger.debug("{} delaying start election".format(self))
            delayerElection = partial(delayers.delayerMethod,
                                      TestPrimaryElector.startElection)
            self.elector.actionQueueStasher.delay(delayerElection(delay))
        elif isinstance(self.primaryDecider, PrimarySelector):
            raise RuntimeError('Does not support nomination since primary is '
                               'selected deterministically')
        else:
            raise RuntimeError('Unknown primary decider encountered {}'.
                               format(self.primaryDecider))

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

    def resetDelaysClient(self):
        logger.debug("{} resetting delays for client".format(self))
        self.nodestack.resetDelays()
        self.clientstack.resetDelays()
        self.clientIbStasher.resetDelays()

    def force_process_delayeds(self):
        c = self.nodestack.force_process_delayeds()
        c += self.nodeIbStasher.force_unstash()
        for r in self.replicas:
            c += r.outBoxTestStasher.force_unstash()
        logger.debug("{} forced processing of delayed messages, "
                     "{} processed in total".format(self, c))
        return c

    def force_process_delayeds_for_client(self):
        c = self.clientstack.force_process_delayeds()
        c += self.clientIbStasher.force_unstash()
        logger.debug("{} forced processing of delayed messages for clients, "
                     "{} processed in total".format(self, c))
        return c

    def reset_delays_and_process_delayeds(self):
        self.resetDelays()
        self.force_process_delayeds()

    def reset_delays_and_process_delayeds_for_clients(self):
        self.resetDelaysClient()
        self.force_process_delayeds_for_client()

    def whitelistNode(self, nodeName: str, *codes: int):
        if nodeName not in self.whitelistedClients:
            self.whitelistedClients[nodeName] = set()
        self.whitelistedClients[nodeName].update(codes)
        logger.debug("{} whitelisting {} for codes {}"
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
        logger.debug("{} whitelisting {} for codes {}"
                     .format(self, clientName, codes))

    def blacklistClient(self, clientName: str,
                        reason: str=None, code: int=None):
        if clientName in self.whitelistedClients:
            # If node whitelisted for all codes
            if len(self.whitelistedClients[clientName]) == 0:
                return
            # If no code is provided or node is whitelisted for that code
            elif code is None or code in self.whitelistedClients[clientName]:
                return
        super().blacklistClient(clientName, reason, code)

    def validateNodeMsg(self, wrappedMsg):
        node_message_factory.set_message_class(TestMsg)
        return super().validateNodeMsg(wrappedMsg)

    async def eatTestMsg(self, msg, frm):
        logger.debug("{0} received Test message: {1} from {2}".
                     format(self.nodestack.name, msg, frm))

    def service_replicas_outbox(self, *args, **kwargs) -> int:
        for r in self.replicas:  # type: TestReplica
            r.outBoxTestStasher.process()
        return super().service_replicas_outbox(*args, **kwargs)

    def ensureKeysAreSetup(self):
        pass

    def getDomainReqHandler(self):
        return TestDomainRequestHandler(self.domainLedger,
                                        self.states[DOMAIN_LEDGER_ID],
                                        self.reqProcessors,
                                        self.bls_bft.bls_store)

    def processRequest(self, request, frm):
        if request.operation[TXN_TYPE] == 'get_buy':
            self.send_ack_to_client(request.key, frm)

            identifier = request.identifier
            buy_key = self.reqHandler.prepare_buy_key(identifier)
            result = self.reqHandler.state.get(buy_key)

            res = {
                f.IDENTIFIER.nm: identifier,
                f.REQ_ID.nm: request.reqId,
                "buy": result
            }

            self.transmitToClient(Reply(res), frm)
        else:
            super().processRequest(request, frm)

node_spyables = [Node.handleOneNodeMsg,
                 Node.handleInvalidClientMsg,
                 Node.processRequest.__name__,
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
                 Node.checkPerformance,
                 Node.processStashedOrderedReqs,
                 Node.lost_master_primary,
                 Node.propose_view_change,
                 Node.getReplyFromLedger,
                 Node.recordAndPropagate,
                 Node.allLedgersCaughtUp,
                 Node.start_catchup,
                 Node.is_catchup_needed,
                 Node.no_more_catchups_needed,
                 Node.caught_up_for_current_view,
                 Node._check_view_change_completed,
                 Node.primary_selected,
                 Node.num_txns_caught_up_in_last_catchup,
                 Node.process_message_req,
                 Node.process_message_rep,
                 Node.request_propagates,
                 Node.send_current_state_to_lagging_node,
                 Node.process_current_state_message,
                 Node._start_view_change_if_possible
                 ]


@spyable(methods=node_spyables)
class TestNode(TestNodeCore, Node):

    def __init__(self, *args, **kwargs):
        self.NodeStackClass = nodeStackClass
        self.ClientStackClass = clientStackClass

        Node.__init__(self, *args, **kwargs)
        TestNodeCore.__init__(self, *args, **kwargs)
        # Balances of all client
        self.balances = {}  # type: Dict[str, int]

        # Txns of all clients, each txn is a tuple like (from, to, amount)
        self.txns = []  # type: List[Tuple]

    @property
    def nodeStackClass(self):
        return getTestableStack(self.NodeStackClass)

    @property
    def clientStackClass(self):
        return getTestableStack(self.ClientStackClass)

    def getLedgerManager(self):
        return TestLedgerManager(
            self,
            ownedByNode=True,
            postAllLedgersCaughtUp=self.allLedgersCaughtUp,
            preCatchupClbk=self.preLedgerCatchUp)

    def sendRepliesToClients(self, committedTxns, ppTime):
        committedTxns = list(committedTxns)
        for txn in committedTxns:
            if txn[TXN_TYPE] == "buy":
                key, value = self.reqHandler.prepare_buy_for_state(txn)
                proof = self.reqHandler.make_proof(key)
                if proof:
                    txn[STATE_PROOF] = proof
        super().sendRepliesToClients(committedTxns, ppTime)

elector_spyables = [
    PrimaryElector.discard,
    PrimaryElector.processPrimary,
    PrimaryElector.sendPrimary
]


@spyable(methods=elector_spyables)
class TestPrimaryElector(PrimaryElector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actionQueueStasher = Stasher(self.actionQueue,
                                          "actionQueueStasher~elector~" +
                                          self.name)

    def _serviceActions(self):
        self.actionQueueStasher.process()
        return super()._serviceActions()


selector_spyables = [PrimarySelector.decidePrimaries]


@spyable(methods=selector_spyables)
class TestPrimarySelector(PrimarySelector):
    pass


replica_spyables = [
    replica.Replica.sendPrePrepare,
    replica.Replica._can_process_pre_prepare,
    replica.Replica.canPrepare,
    replica.Replica.validatePrepare,
    replica.Replica.addToPrePrepares,
    replica.Replica.processPrePrepare,
    replica.Replica.processPrepare,
    replica.Replica.processCommit,
    replica.Replica.doPrepare,
    replica.Replica.doOrder,
    replica.Replica.discard,
    replica.Replica.stashOutsideWatermarks,
    replica.Replica.revert_unordered_batches,
    replica.Replica.can_process_since_view_change_in_progress,
    replica.Replica.processThreePhaseMsg,
    replica.Replica.process_requested_pre_prepare,
    replica.Replica._request_pre_prepare_for_prepare,
    replica.Replica.is_pre_prepare_time_correct,
    replica.Replica.is_pre_prepare_time_acceptable,
    replica.Replica._process_stashed_pre_prepare_for_time_if_possible,
]


@spyable(methods=replica_spyables)
class TestReplica(replica.Replica):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Each TestReplica gets it's own outbox stasher, all of which TestNode
        # processes in its overridden serviceReplicaOutBox
        self.outBoxTestStasher = \
            Stasher(self.outBox, "replicaOutBoxTestStasher~" + self.name)


class TestReplicas(Replicas):
    def _new_replica(self, instance_id: int, is_master: bool, bls_bft: BlsBft):
        return TestReplica(self._node, instance_id, is_master, bls_bft)


class TestNodeSet(ExitStack):

    def __init__(self,
                 names: Iterable[str]=None,
                 count: int=None,
                 nodeReg=None,
                 tmpdir=None,
                 keyshare=True,
                 primaryDecider=None,
                 pluginPaths: Iterable[str]=None,
                 testNodeClass=TestNode):

        super().__init__()
        self.tmpdir = tmpdir
        self.keyshare = keyshare
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

        seed = randomSeed()
        if self.keyshare:
            learnKeysFromOthers(self.tmpdir, name, self.nodes.values())

        testNodeClass = self.testNodeClass
        node = self.enter_context(
            testNodeClass(name=name,
                          ha=ha,
                          cliname=cliname,
                          cliha=cliha,
                          nodeRegistry=copy(self.nodeReg),
                          basedirpath=self.tmpdir,
                          base_data_dir=self.tmpdir,
                          primaryDecider=self.primaryDecider,
                          pluginPaths=self.pluginPaths,
                          seed=seed))

        if self.keyshare:
            tellKeysToOthers(node, self.nodes.values())

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

    def __getitem__(self, key) -> Optional[TestNode]:
        if key in self.nodes:
            return self.nodes[key]
        elif isinstance(key, int):
            return list(self.nodes.values())[key]
        else:
            return None

    def __len__(self):
        return self.nodes.__len__()

    @property
    def nodeNames(self):
        return sorted(self.nodes.keys())

    @property
    def nodes_by_rank(self):
        return [t[1] for t in sorted([(node.rank, node)
                                      for node in self.nodes.values()],
                                     key=operator.itemgetter(0))]

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

    def getAllMsgReceived(self, node: NodeRef, method: str = None) -> List:
        return getAllMsgReceivedForNode(self.getNode(node), method)


monitor_spyables = [Monitor.isMasterThroughputTooLow,
                    Monitor.isMasterReqLatencyTooHigh,
                    Monitor.sendThroughput,
                    Monitor.requestOrdered,
                    Monitor.reset,
                    Monitor.warn_has_lot_unordered_requests
                    ]


@spyable(methods=monitor_spyables)
class TestMonitor(Monitor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.masterReqLatenciesTest = {}

    def requestOrdered(self, reqIdrs: List[Tuple[str, int]], instId: int,
                       byMaster: bool = False):
        durations = super().requestOrdered(reqIdrs, instId, byMaster)
        if byMaster and durations:
            for (identifier, reqId), duration in durations.items():
                self.masterReqLatenciesTest[identifier, reqId] = duration

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
                # for n in nodeset:
                #     n.startKeySharing()
                ctx = adict(looper=looper, nodeset=nodeset, tmpdir=tmpdir)
                looper.run(checkNodesConnected(nodeset))
                ensureElectionsDone(looper=looper,
                                    nodes=nodeset)
                looper.run(coro(ctx))

    def fresh_tdir(self):
        return self.tmpdir_factory.mktemp('').strpath


class MockedNodeStack:
    def remotesByConnected(self):
        return [], []


class MockedBlacklister:
    def isBlacklisted(self, remote):
        return True


def checkPoolReady(looper: Looper,
                   nodes: Sequence[TestNode],
                   customTimeout=None):
    """
    Check that pool is in Ready state
    """

    timeout = customTimeout or waits.expectedPoolStartUpTimeout(len(nodes))
    looper.run(
        eventually(checkNodesAreReady, nodes,
                   retryWait=.25,
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
                              customTimeout=None):
    expectedRemoteState = expectedRemoteState if expectedRemoteState else CONNECTED
    # run for how long we expect all of the connections to take
    timeout = customTimeout or waits.expectedPoolInterconnectionTime(
        len(stacks))
    logger.debug(
        "waiting for {} seconds to check connections...".format(timeout))
    # verify every node can see every other as a remote
    funcs = [
        partial(checkRemoteExists, frm.nodestack, to.name, expectedRemoteState)
        for frm, to in permutations(stacks, 2)]
    await eventuallyAll(*funcs,
                        retryWait=.5,
                        totalTimeout=timeout,
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
            raise Exception(
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
        unknowns = [r for r in replicas if r.primaryName is None]
        assert len(unknowns) == 0, "election should be complete, " \
            "but {} out of {} ({}) don't know who the primary " \
            "is for protocol instance {}".\
            format(len(unknowns), len(replicas), unknowns, replicas[0].instId)

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
        assert node.isReady(), '{} has status {}'.format(node, node.status)


async def checkNodesParticipating(nodes: Sequence[TestNode], timeout: int=None):
    # TODO is this used? If so - add timeout for it to plenum.test.waits
    if not timeout:
        timeout = .75 * len(nodes)

    def chk():
        for node in nodes:
            assert node.isParticipating

    await eventually(chk, retryWait=1, timeout=timeout)


def checkEveryProtocolInstanceHasOnlyOnePrimary(looper: Looper,
                                                nodes: Sequence[TestNode],
                                                retryWait: float = None,
                                                timeout: float = None,
                                                numInstances: int = None):

    coro = eventually(instances, nodes, numInstances,
                      retryWait=retryWait, timeout=timeout)
    insts, timeConsumed = timeThis(looper.run, coro)
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
                                      customTimeout: float = None):
    def checkAtMostOnePrim(node):
        prims = [r for r in node.replicas if r.isPrimary]
        assert len(prims) <= 1

    timeout = customTimeout or waits.expectedPoolElectionTimeout(len(nodes))
    for node in nodes:
        looper.run(eventually(checkAtMostOnePrim,
                              node,
                              retryWait=retryWait,
                              timeout=timeout))


def checkProtocolInstanceSetup(looper: Looper,
                               nodes: Sequence[TestNode],
                               retryWait: float = 1,
                               customTimeout: float = None,
                               numInstances: int = None):

    timeout = customTimeout or waits.expectedPoolElectionTimeout(len(nodes))

    checkEveryProtocolInstanceHasOnlyOnePrimary(looper=looper,
                                                nodes=nodes,
                                                retryWait=retryWait,
                                                timeout=timeout,
                                                numInstances=numInstances)

    checkEveryNodeHasAtMostOnePrimary(looper=looper,
                                      nodes=nodes,
                                      retryWait=retryWait,
                                      customTimeout=timeout)

    primaryReplicas = {replica.instId: replica
                       for node in nodes
                       for replica in node.replicas if replica.isPrimary}
    return [r[1] for r in
            sorted(primaryReplicas.items(), key=operator.itemgetter(0))]


def ensureElectionsDone(looper: Looper,
                        nodes: Sequence[TestNode],
                        retryWait: float = None,  # seconds
                        customTimeout: float = None,
                        numInstances: int = None) -> Sequence[TestNode]:
    # TODO: Change the name to something like `ensure_primaries_selected`
    # since there might not always be an election, there might be a round
    # robin selection
    """
    Wait for elections to be complete

    :param retryWait:
    :param customTimeout: specific timeout
    :param numInstances: expected number of protocol instances
    :return: primary replica for each protocol instance
    """

    if retryWait is None:
        retryWait = 1

    if customTimeout is None:
        customTimeout = waits.expectedPoolElectionTimeout(len(nodes))

    return checkProtocolInstanceSetup(
        looper=looper,
        nodes=nodes,
        retryWait=retryWait,
        customTimeout=customTimeout,
        numInstances=numInstances)


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


def instances(nodes: Sequence[Node],
              numInstances: int = None) -> Dict[int, List[replica.Replica]]:
    numInstances = (getRequiredInstances(len(nodes))
                    if numInstances is None else numInstances)
    for n in nodes:
        assert len(n.replicas) == numInstances
    return {i: [n.replicas[i] for n in nodes] for i in range(numInstances)}


def getRequiredInstances(nodeCount: int) -> int:
    f_value = getMaxFailures(nodeCount)
    return f_value + 1


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


def get_master_primary_node(nodes):
    node = next(iter(nodes))
    if node.replicas[0].primaryName is not None:
        nm = TestReplica.getNodeName(node.replicas[0].primaryName)
        return nodeByName(nodes, nm)
    raise AssertionError('No primary found for master')


def get_last_master_non_primary_node(nodes):
    return getNonPrimaryReplicas(nodes)[-1].node


def get_first_master_non_primary_node(nodes):
    return getNonPrimaryReplicas(nodes)[0].node


def primaryNodeNameForInstance(nodes, instanceId):
    primaryNames = {node.replicas[instanceId].primaryName for node in nodes}
    assert 1 == len(primaryNames)
    primaryReplicaName = next(iter(primaryNames))
    return primaryReplicaName[:-2]


def nodeByName(nodes, name):
    for node in nodes:
        if node.name == name:
            return node
    raise Exception("Node with the name '{}' has not been found.".format(name))


def check_node_disconnected_from(needle: str, haystack: Iterable[TestNode]):
    """
    Check if the node name given by `needle` is disconnected from nodes in
    `haystack`
    :param needle: Node name which should be disconnected from nodes from
    `haystack`
    :param haystack: nodes who should be disconnected from `needle`
    :return:
    """
    assert all([needle not in node.nodestack.connecteds for node in haystack])


def ensure_node_disconnected(looper, disconnected, other_nodes,
                             timeout=None):
    timeout = timeout or (len(other_nodes) - 1)
    disconnected_name = disconnected if isinstance(disconnected, str) \
        else disconnected.name
    looper.run(eventually(check_node_disconnected_from, disconnected_name,
                          [n for n in other_nodes
                           if n.name != disconnected_name],
                          retryWait=1, timeout=timeout))

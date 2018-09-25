import operator
import time
import types
from collections import OrderedDict
from contextlib import ExitStack
from functools import partial
from itertools import combinations
from typing import Iterable, Iterator, Tuple, Sequence, Dict, TypeVar, \
    List, Optional

from crypto.bls.bls_bft import BlsBft
from plenum.common.request import Request
from plenum.common.stacks import nodeStackClass, clientStackClass
from plenum.common.txn_util import get_from, get_req_id, get_payload_data, get_type
from plenum.server.client_authn import CoreAuthNr
from plenum.server.domain_req_handler import DomainRequestHandler
from plenum.server.propagator import Requests
from stp_core.crypto.util import randomSeed
from stp_core.network.port_dispenser import genHa

import plenum.test.delayers as delayers
from common.error import error
from stp_core.loop.eventually import eventually, eventuallyAll
from stp_core.network.exceptions import RemoteNotFound
from plenum.common.keygen_utils import learnKeysFromOthers, tellKeysToOthers
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.common.startable import Status
from plenum.common.types import NodeDetail, f
from plenum.common.constants import CLIENT_STACK_SUFFIX, TXN_TYPE, \
    DOMAIN_LEDGER_ID, STATE_PROOF
from plenum.common.util import Seconds, getMaxFailures
from stp_core.common.util import adict
from plenum.server import replica
from plenum.server.instances import Instances
from plenum.server.monitor import Monitor
from plenum.server.node import Node
from plenum.server.view_change.view_changer import ViewChanger
from plenum.server.primary_elector import PrimaryElector
from plenum.server.primary_selector import PrimarySelector
from plenum.test.greek import genNodeNames
from plenum.test.msgs import TestMsg
from plenum.test.spy_helpers import getLastMsgReceivedForNode, \
    getAllMsgReceivedForNode, getAllArgs
from plenum.test.stasher import Stasher
from plenum.test.test_ledger_manager import TestLedgerManager
from plenum.test.test_stack import StackedTester, getTestableStack, \
    RemoteState, checkState
from plenum.test.testable import spyable
from plenum.test import waits
from plenum.common.messages.node_message_factory import node_message_factory
from plenum.server.replicas import Replicas
from plenum.common.config_helper import PNodeConfigHelper
from hashlib import sha256
from plenum.common.messages.node_messages import Reply

logger = getlogger()


@spyable(methods=[CoreAuthNr.authenticate])
class TestCoreAuthnr(CoreAuthNr):
    write_types = CoreAuthNr.write_types.union({'buy', 'randombuy'})
    query_types = CoreAuthNr.query_types.union({'get_buy', })


class TestDomainRequestHandler(DomainRequestHandler):
    write_types = DomainRequestHandler.write_types.union({'buy', 'randombuy', })
    query_types = DomainRequestHandler.query_types.union({'get_buy', })

    @staticmethod
    def prepare_buy_for_state(txn):
        from common.serializers.serialization import domain_state_serializer
        identifier = get_from(txn)
        req_id = get_req_id(txn)
        value = domain_state_serializer.serialize({"amount": get_payload_data(txn)['amount']})
        key = TestDomainRequestHandler.prepare_buy_key(identifier, req_id)
        return key, value

    @staticmethod
    def prepare_buy_key(identifier, req_id):
        return sha256('{}{}:buy'.format(identifier, req_id).encode()).digest()

    def _updateStateWithSingleTxn(self, txn, isCommitted=False):
        typ = get_type(txn)
        if typ == 'buy':
            key, value = self.prepare_buy_for_state(txn)
            self.state.set(key, value)
            logger.trace('{} after adding to state, headhash is {}'.
                         format(self, self.state.headHash))
        else:
            super()._updateStateWithSingleTxn(txn, isCommitted=isCommitted)

    def gen_txn_path(self, txn):
        return None


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
        self.whitelistedNodes = {}  # type: Dict[str, Set[int]]

        # Clients that wont be blacklisted by this node if the suspicion code
        # is among the set of suspicion codes mapped to its name. If the set of
        # suspicion codes is empty then the client would not be blacklisted for
        #  suspicion code
        self.whitelistedClients = {}  # type: Dict[str, Set[int]]

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
        for i in self.replicas.keys():
            self.monitor.addInstance(i)
        self.replicas._monitor = self.monitor
        self.replicas.register_monitor_handler()

    def create_replicas(self, config=None):
        return TestReplicas(self, self.monitor, config, self.metrics)

    async def processNodeInBox(self):
        self.nodeIbStasher.process()
        await super().processNodeInBox()

    async def processClientInBox(self):
        self.clientIbStasher.process()
        await super().processClientInBox()

    def _serviceActions(self):
        self.actionQueueStasher.process()
        return super()._serviceActions()

    def newPrimaryDecider(self):
        pdCls = self.primaryDecider if self.primaryDecider else \
            TestPrimarySelector
        return pdCls(self)

    def newViewChanger(self):
        vchCls = self.view_changer if self.view_changer is not None else \
            TestViewChanger
        return vchCls(self)

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

    def resetDelays(self, *names):
        logger.debug("{} resetting delays".format(self))
        self.nodestack.resetDelays()
        self.nodeIbStasher.resetDelays(*names)
        for r in self.replicas.values():
            r.outBoxTestStasher.resetDelays()

    def resetDelaysClient(self):
        logger.debug("{} resetting delays for client".format(self))
        self.nodestack.resetDelays()
        self.clientstack.resetDelays()
        self.clientIbStasher.resetDelays()

    def force_process_delayeds(self, *names):
        c = self.nodestack.force_process_delayeds(*names)
        c += self.nodeIbStasher.force_unstash(*names)
        for r in self.replicas.values():
            c += r.outBoxTestStasher.force_unstash(*names)
        logger.debug("{} forced processing of delayed messages, "
                     "{} processed in total".format(self, c))
        return c

    def force_process_delayeds_for_client(self):
        c = self.clientstack.force_process_delayeds()
        c += self.clientIbStasher.force_unstash()
        logger.debug("{} forced processing of delayed messages for clients, "
                     "{} processed in total".format(self, c))
        return c

    def reset_delays_and_process_delayeds(self, *names):
        self.resetDelays(*names)
        self.force_process_delayeds(*names)

    def reset_delays_and_process_delayeds_for_clients(self):
        self.resetDelaysClient()
        self.force_process_delayeds_for_client()

    def whitelistNode(self, nodeName: str, *codes: int):
        if nodeName not in self.whitelistedClients:
            self.whitelistedClients[nodeName] = set()
        self.whitelistedClients[nodeName].update(codes)
        logger.debug("{} whitelisting {} for codes {}"
                     .format(self, nodeName, codes))

    def blacklistNode(self, nodeName: str, reason: str = None, code: int = None):
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
                        reason: str = None, code: int = None):
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
        for r in self.replicas.values():  # type: TestReplica
            r.outBoxTestStasher.process()
        return super().service_replicas_outbox(*args, **kwargs)

    def ensureKeysAreSetup(self):
        pass

    def getDomainReqHandler(self):
        return TestDomainRequestHandler(self.domainLedger,
                                        self.states[DOMAIN_LEDGER_ID],
                                        self.config, self.reqProcessors,
                                        self.bls_bft.bls_store,
                                        self.getStateTsDbStorage())

    def init_core_authenticator(self):
        state = self.getState(DOMAIN_LEDGER_ID)
        return TestCoreAuthnr(state=state)

    def processRequest(self, request, frm):
        if request.operation[TXN_TYPE] == 'get_buy':
            self.send_ack_to_client(request.key, frm)

            identifier = request.identifier
            req_id = request.reqId
            req_handler = self.get_req_handler(DOMAIN_LEDGER_ID)
            buy_key = req_handler.prepare_buy_key(identifier, req_id)
            result = req_handler.state.get(buy_key)

            res = {
                f.IDENTIFIER.nm: identifier,
                f.REQ_ID.nm: req_id,
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
                 Node.discard,
                 Node.reportSuspiciousNode,
                 Node.reportSuspiciousClient,
                 Node.processPropagate,
                 Node.propagate,
                 Node.forward,
                 Node.send,
                 Node.checkPerformance,
                 Node.processStashedOrderedReqs,
                 Node.lost_master_primary,
                 Node.propose_view_change,
                 Node.getReplyFromLedger,
                 Node.getReplyFromLedgerForRequest,
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
                 Node.transmitToClient,
                 Node.has_ordered_till_last_prepared_certificate,
                 Node.on_inconsistent_3pc_state
                 ]


@spyable(methods=node_spyables)
class TestNode(TestNodeCore, Node):

    def __init__(self, *args, **kwargs):
        from plenum.common.stacks import nodeStackClass, clientStackClass
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

    def get_new_ledger_manager(self):
        return TestLedgerManager(
            self,
            ownedByNode=True,
            postAllLedgersCaughtUp=self.allLedgersCaughtUp,
            preCatchupClbk=self.preLedgerCatchUp,
            postCatchupClbk=self.postLedgerCatchUp,
            ledger_sync_order=self.ledger_ids,
            metrics=self.metrics
        )

    def sendRepliesToClients(self, committedTxns, ppTime):
        committedTxns = list(committedTxns)
        req_handler = self.get_req_handler(DOMAIN_LEDGER_ID)
        for txn in committedTxns:
            if get_type(txn) == "buy":
                key, value = req_handler.prepare_buy_for_state(txn)
                _, proof = req_handler.get_value_from_state(key, with_proof=True)
                if proof:
                    txn[STATE_PROOF] = proof
        super().sendRepliesToClients(committedTxns, ppTime)

    def schedule_node_status_dump(self):
        pass

    def dump_additional_info(self):
        pass

    def restart_clientstack(self):
        self.clientstack.restart()


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


view_changer_spyables = [
    ViewChanger.sendInstanceChange,
    ViewChanger._start_view_change_if_possible,
    ViewChanger.process_instance_change_msg,
    ViewChanger.startViewChange,
    ViewChanger.process_future_view_vchd_msg
]


@spyable(methods=view_changer_spyables)
class TestViewChanger(ViewChanger):
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
    replica.Replica.revert,
    replica.Replica.can_process_since_view_change_in_progress,
    replica.Replica.processThreePhaseMsg,
    replica.Replica._request_pre_prepare,
    replica.Replica._request_pre_prepare_for_prepare,
    replica.Replica._request_prepare,
    replica.Replica._request_commit,
    replica.Replica.process_requested_pre_prepare,
    replica.Replica.process_requested_prepare,
    replica.Replica.process_requested_commit,
    replica.Replica.is_pre_prepare_time_correct,
    replica.Replica.is_pre_prepare_time_acceptable,
    replica.Replica._process_stashed_pre_prepare_for_time_if_possible,
    replica.Replica.markCheckPointStable,
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
    _replica_class = TestReplica

    def _new_replica(self, instance_id: int, is_master: bool, bls_bft: BlsBft):
        return self.__class__._replica_class(self._node, instance_id,
                                                self._config, is_master,
                                                bls_bft, self._metrics)


# TODO: probably delete when remove from node
class TestNodeSet(ExitStack):

    def __init__(self,
                 config,
                 names: Iterable[str] = None,
                 count: int = None,
                 nodeReg=None,
                 tmpdir=None,
                 keyshare=True,
                 primaryDecider=None,
                 pluginPaths: Iterable[str] = None,
                 testNodeClass=TestNode):

        super().__init__()
        self.tmpdir = tmpdir
        assert config is not None
        self.config = config
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

        config_helper = PNodeConfigHelper(name, self.config, chroot=self.tmpdir)

        seed = randomSeed()
        if self.keyshare:
            learnKeysFromOthers(config_helper.keys_dir, name, self.nodes.values())

        testNodeClass = self.testNodeClass
        node = self.enter_context(
            testNodeClass(name=name,
                          ha=ha,
                          cliname=cliname,
                          cliha=cliha,
                          config_helper=config_helper,
                          primaryDecider=self.primaryDecider,
                          pluginPaths=self.pluginPaths,
                          seed=seed))

        if self.keyshare:
            tellKeysToOthers(node, self.nodes.values())

        self.nodes[name] = node
        self.__dict__[name] = node
        return node

    def removeNode(self, name):
        self.nodes[name].stop()
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
                    Monitor.reset
                    ]


@spyable(methods=monitor_spyables)
class TestMonitor(Monitor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.masterReqLatenciesTest = {}

    def requestOrdered(self, reqIdrs: List[str],
                       instId: int, requests, byMaster: bool = False) -> Dict:
        durations = super().requestOrdered(reqIdrs, instId, requests, byMaster)
        if byMaster and durations:
            for key, duration in durations.items():
                self.masterReqLatenciesTest[key] = duration

    def reset(self):
        super().reset()
        self.masterReqLatenciesTest = {}


class Pool:
    def __init__(self, tmpdir=None, tmpdir_factory=None, config=None, testNodeSetClass=TestNodeSet):
        self.tmpdir = tmpdir
        self.tmpdir_factory = tmpdir_factory
        self.testNodeSetClass = testNodeSetClass
        self.config = config
        self.is_run = False

    def run(self, coro, nodecount=4):
        assert self.is_run == False

        self.is_run = True
        tmpdir = self.tmpdir if self.tmpdir is not None else self.fresh_tdir()
        with self.testNodeSetClass(self.config, count=nodecount, tmpdir=tmpdir) as nodeset:
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


async def checkNodesConnected(nodes: Iterable[TestNode],
                              customTimeout=None):
    # run for how long we expect all of the connections to take
    timeout = customTimeout or \
              waits.expectedPoolInterconnectionTime(len(nodes))
    logger.debug(
        "waiting for {} seconds to check connections...".format(timeout))
    # verify every node can see every other as a remote
    funcs = [partial(check_node_connected, n, set(nodes) - {n}) for n in nodes]
    await eventuallyAll(*funcs,
                        retryWait=.5,
                        totalTimeout=timeout,
                        acceptableExceptions=[AssertionError, RemoteNotFound])


def checkNodeRemotes(node: TestNode, states: Dict[str, RemoteState] = None,
                     state: RemoteState = None):
    assert states or state, "either state or states is required"
    assert not (
            states and state), "only one of state or states should be provided, " \
                               "but not both"
    for remote in node.nodestack.remotes.values():
        try:
            s = states[remote.name] if states else state
            checkState(s, remote, "{}'s remote {}".format(node, remote.name))
        except Exception as ex:
            logger.debug("state checking exception is {} and args are {}"
                         "".format(ex, ex.args))
            raise Exception(
                "Error with {} checking remote {} in {}".format(node.name,
                                                                remote.name,
                                                                states
                                                                )) from ex


def checkIfSameReplicaIsPrimary(looper: Looper,
                                replicas: Sequence[TestReplica] = None,
                                retryWait: float = 1,
                                timeout: float = 20):
    # One and only one primary should be found and every replica should agree
    # on same primary

    def checkElectionDone():
        unknowns = [r for r in replicas if r.primaryName is None]
        assert len(unknowns) == 0, "election should be complete, " \
                                   "but {} out of {} ({}) don't know who the primary " \
                                   "is for protocol instance {}". \
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


async def checkNodesParticipating(nodes: Sequence[TestNode], timeout: int = None):
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
        checkIfSameReplicaIsPrimary(looper=looper,
                                    replicas=replicas,
                                    retryWait=retryWait,
                                    timeout=newTimeout)


def checkEveryNodeHasAtMostOnePrimary(looper: Looper,
                                      nodes: Sequence[TestNode],
                                      retryWait: float = None,
                                      customTimeout: float = None):
    def checkAtMostOnePrim(node):
        prims = [r for r in node.replicas.values() if r.isPrimary]
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
                       for replica in node.replicas.values() if replica.isPrimary}
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


def prepareNodeSet(looper: Looper, txnPoolNodeSet):
    # TODO: Come up with a more specific name for this

    # Key sharing party
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Remove all the nodes
    for n in list(txnPoolNodeSet):
        looper.removeProdable(txnPoolNodeSet)
        txnPoolNodeSet.remove(n)


def checkViewChangeInitiatedForNode(node: TestNode, proposedViewNo: int):
    """
    Check if view change initiated for a given node
    :param node: The node to check for
    :param proposedViewNo: The view no which is proposed
    :return:
    """
    params = [args for args in getAllArgs(node.view_changer, ViewChanger.startViewChange)]
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


def check_node_connected(connected: TestNode,
                         other_nodes: Iterable[TestNode]):
    """
    Check if the node `connected` is connected to `other_nodes`
    :param connected: node which should be connected to other nodes and clients
    :param other_nodes: nodes who should be connected to `connected`
    """
    assert connected.nodestack.opened
    assert connected.clientstack.opened
    assert all([connected.name in other.nodestack.connecteds
                for other in other_nodes])


def check_node_disconnected(disconnected: TestNode,
                            other_nodes: Iterable[TestNode]):
    """
    Check if the node `disconnected` is disconnected from `other_nodes`
    :param disconnected: node which should be disconnected from other nodes
    and clients
    :param other_nodes: nodes who should be disconnected from `disconnected`
    """
    assert not disconnected.nodestack.opened
    assert not disconnected.clientstack.opened
    assert all([disconnected.name not in other.nodestack.connecteds
                for other in other_nodes])


def ensure_node_disconnected(looper: Looper,
                             disconnected: TestNode,
                             other_nodes: Iterable[TestNode],
                             timeout: float = None):
    timeout = timeout or (len(other_nodes) - 1)
    looper.run(eventually(check_node_disconnected, disconnected,
                          other_nodes, retryWait=1, timeout=timeout))

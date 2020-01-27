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
from plenum.common.stashing_router import StashingRouter
from plenum.common.txn_util import get_type
from plenum.server.client_authn import CoreAuthNr
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.server.consensus.monitoring.primary_connection_monitor_service import PrimaryConnectionMonitorService
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.server.consensus.view_change_trigger_service import ViewChangeTriggerService
from plenum.server.node_bootstrap import NodeBootstrap
from plenum.test.buy_handler import BuyHandler
from plenum.test.constants import GET_BUY
from plenum.test.get_buy_handler import GetBuyHandler
from plenum.test.random_buy_handler import RandomBuyHandler
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
from plenum.common.messages.node_messages import Reply

logger = getlogger()


@spyable(methods=[CoreAuthNr.authenticate])
class TestCoreAuthnr(CoreAuthNr):
    pass


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

    def init_core_authenticator(self):
        state = self.getState(DOMAIN_LEDGER_ID)
        return TestCoreAuthnr(self.write_manager.txn_types,
                              self.read_manager.txn_types,
                              self.action_manager.txn_types,
                              state=state)

    def processRequest(self, request, frm):
        if request.operation[TXN_TYPE] == GET_BUY:
            self.send_ack_to_client(request.key, frm)

            identifier = request.identifier
            req_id = request.reqId
            result = self.read_manager.get_result(request)

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
                 Node.getReplyFromLedger,
                 Node.getReplyFromLedgerForRequest,
                 Node.recordAndPropagate,
                 Node.allLedgersCaughtUp,
                 Node.start_catchup,
                 Node._do_start_catchup,
                 Node.is_catchup_needed,
                 Node.no_more_catchups_needed,
                 Node.primary_selected,
                 Node.num_txns_caught_up_in_last_catchup,
                 Node.process_message_req,
                 Node.process_message_rep,
                 Node.request_propagates,
                 Node.transmitToClient,
                 Node.has_ordered_till_last_prepared_certificate,
                 Node.on_inconsistent_3pc_state]

class TestNodeBootstrap(NodeBootstrap):

    def _register_domain_req_handlers(self):
        super()._register_domain_req_handlers()
        self.node.write_manager.register_req_handler(BuyHandler(self.node.db_manager))
        self.node.write_manager.register_req_handler(RandomBuyHandler(self.node.db_manager))
        self.node.read_manager.register_req_handler(GetBuyHandler(self.node.db_manager))

    def _init_common_managers(self):
        super()._init_common_managers()
        self.node.ledgerManager = TestLedgerManager(self.node,
                                                    postAllLedgersCaughtUp=self.node.allLedgersCaughtUp,
                                                    preCatchupClbk=self.node.preLedgerCatchUp,
                                                    postCatchupClbk=self.node.postLedgerCatchUp,
                                                    ledger_sync_order=self.node.ledger_ids,
                                                    metrics=self.node.metrics)


@spyable(methods=node_spyables)
class TestNode(TestNodeCore, Node):

    def __init__(self, *args, **kwargs):
        from plenum.common.stacks import nodeStackClass, clientStackClass
        self.NodeStackClass = nodeStackClass
        self.ClientStackClass = clientStackClass
        if kwargs.get('bootstrap_cls', None) is None:
            kwargs['bootstrap_cls'] = TestNodeBootstrap

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

    def sendRepliesToClients(self, committedTxns, ppTime):
        committedTxns = list(committedTxns)
        handler = None
        for h in self.read_manager.request_handlers.values():
            if isinstance(h, GetBuyHandler):
                handler = h
        if handler:
            for txn in committedTxns:
                if get_type(txn) == "buy":
                    key, value = handler.prepare_buy_for_state(txn)
                    _, proof = handler._get_value_from_state(key, with_proof=True)
                    if proof:
                        txn[STATE_PROOF] = proof

        super().sendRepliesToClients(committedTxns, ppTime)

    def schedule_node_status_dump(self):
        pass

    def dump_additional_info(self):
        pass

    def restart_clientstack(self):
        self.clientstack.restart()


replica_stasher_spyables = [
    StashingRouter._stash,
    StashingRouter._process,
    StashingRouter.discard
]


@spyable(methods=replica_stasher_spyables)
class TestStashingRouter(StashingRouter):
    pass


replica_spyables = [
    replica.Replica.revert_unordered_batches,
    replica.Replica._send_ordered
]


@spyable(methods=replica_spyables)
class TestReplica(replica.Replica):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Each TestReplica gets it's own outbox stasher, all of which TestNode
        # processes in its overridden serviceReplicaOutBox
        self.outBoxTestStasher = \
            Stasher(self.outBox, "replicaOutBoxTestStasher~" + self.name)

    def _init_replica_stasher(self):
        return TestStashingRouter(self.config.REPLICA_STASH_LIMIT,
                                  buses=[self.internal_bus, self._external_bus],
                                  unstash_handler=self._add_to_inbox)

    def _init_checkpoint_service(self) -> CheckpointService:
        return TestCheckpointService(data=self._consensus_data,
                                     bus=self.internal_bus,
                                     network=self._external_bus,
                                     stasher=self.stasher,
                                     db_manager=self.node.db_manager,
                                     metrics=self.metrics)

    def _init_ordering_service(self) -> OrderingService:
        return TestOrderingService(self._consensus_data,
                                   timer=self.node.timer,
                                   bus=self.internal_bus,
                                   network=self._external_bus,
                                   write_manager=self.node.write_manager,
                                   bls_bft_replica=self._bls_bft_replica,
                                   freshness_checker=self._freshness_checker,
                                   get_current_time=self.get_current_time,
                                   get_time_for_3pc_batch=self.get_time_for_3pc_batch,
                                   stasher=self.stasher,
                                   metrics=self.metrics)

    def _init_view_change_service(self) -> ViewChangeService:
        return TestViewChangeService(data=self._consensus_data,
                                     timer=self.node.timer,
                                     bus=self.internal_bus,
                                     network=self._external_bus,
                                     stasher=self.stasher,
                                     primaries_selector=self.node.primaries_selector)

    def _init_view_change_trigger_service(self) -> Optional[ViewChangeTriggerService]:
        if not self.isMaster:
            return

        return TestViewChangeTriggerService(data=self._consensus_data,
                                            timer=self.node.timer,
                                            bus=self.internal_bus,
                                            network=self._external_bus,
                                            db_manager=self.node.db_manager,
                                            stasher=self.stasher,
                                            is_master_degraded=self.node.monitor.isMasterDegraded,
                                            metrics=self.metrics)

    def _init_primary_connection_monitor_service(self) -> PrimaryConnectionMonitorService:
        return TestPrimaryConnectionMonitorService(data=self._consensus_data,
                                                   timer=self.node.timer,
                                                   bus=self.internal_bus,
                                                   network=self._external_bus,
                                                   metrics=self.metrics)

    def _init_message_req_service(self) -> MessageReqService:
        return TestMessageReqService(data=self._consensus_data,
                                     bus=self.internal_bus,
                                     network=self._external_bus,
                                     metrics=self.metrics)


message_req_spyables = [
    MessageReqService.process_message_req,
    MessageReqService.process_message_rep,
    MessageReqService.process_missing_message,
]


@spyable(methods=message_req_spyables)
class TestMessageReqService(MessageReqService):
    pass


checkpointer_spyables = [
    CheckpointService.set_watermarks,
    CheckpointService._mark_checkpoint_stable,
    CheckpointService.process_checkpoint,
    CheckpointService.discard,
]


@spyable(methods=checkpointer_spyables)
class TestCheckpointService(CheckpointService):
    pass


ordering_service_spyables = [
    OrderingService._order_3pc_key,
    OrderingService._can_prepare,
    OrderingService._is_pre_prepare_time_correct,
    OrderingService._is_pre_prepare_time_acceptable,
    OrderingService._process_stashed_pre_prepare_for_time_if_possible,
    OrderingService._request_propagates_if_needed,
    OrderingService.revert_unordered_batches,
    OrderingService._request_pre_prepare_for_prepare,
    OrderingService._order_3pc_key,
    OrderingService._request_pre_prepare,
    OrderingService._request_prepare,
    OrderingService._request_commit,
    OrderingService.send_pre_prepare,
    OrderingService._can_process_pre_prepare,
    OrderingService._can_prepare,
    OrderingService._validate_prepare,
    OrderingService._add_to_pre_prepares,
    OrderingService.process_preprepare,
    OrderingService.process_prepare,
    OrderingService.process_commit,
    OrderingService._do_prepare,
    OrderingService._do_order,
    OrderingService._revert,
    OrderingService._validate,
    OrderingService.post_batch_rejection,
    OrderingService.post_batch_creation,
    OrderingService.process_old_view_preprepare_reply,
    OrderingService.report_suspicious_node
]


@spyable(methods=ordering_service_spyables)
class TestOrderingService(OrderingService):
    pass


view_change_service_spyables = [
    ViewChangeService._finish_view_change
]


@spyable(methods=view_change_service_spyables)
class TestViewChangeService(ViewChangeService):
    pass


view_change_trigger_service_spyables = [
    ViewChangeTriggerService.process_instance_change,
    ViewChangeTriggerService._send_instance_change
]


@spyable(methods=view_change_trigger_service_spyables)
class TestViewChangeTriggerService(ViewChangeTriggerService):
    pass


primary_connection_monitor_service_spyables = [
    PrimaryConnectionMonitorService._primary_disconnected
]


@spyable(methods=primary_connection_monitor_service_spyables)
class TestPrimaryConnectionMonitorService(PrimaryConnectionMonitorService):
    pass


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
                              customTimeout=20):
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
                               "for protocol no {}, Replicas: {}" \
            .format(pris, replicas[0].instId, [{r.name: r.primaryName} for r in replicas])

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
                                                instances_list: Sequence[int] = None):
    coro = eventually(instances, nodes, instances_list,
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


def check_not_in_view_change(nodes):
    assert all([not n.master_replica._consensus_data.waiting_for_new_view for n in nodes])


def checkProtocolInstanceSetup(looper: Looper,
                               nodes: Sequence[TestNode],
                               retryWait: float = 1,
                               customTimeout: float = None,
                               instances: Sequence[int] = None,
                               check_primaries=True):
    timeout = customTimeout or waits.expectedPoolElectionTimeout(len(nodes))

    checkEveryProtocolInstanceHasOnlyOnePrimary(looper=looper,
                                                nodes=nodes,
                                                retryWait=retryWait,
                                                timeout=timeout,
                                                instances_list=instances)

    checkEveryNodeHasAtMostOnePrimary(looper=looper,
                                      nodes=nodes,
                                      retryWait=retryWait,
                                      customTimeout=timeout)

    looper.run(eventually(check_not_in_view_change, nodes, retryWait=retryWait, timeout=customTimeout))

    if check_primaries:
        for n in nodes[1:]:
            assert nodes[0].primaries == n.primaries

    primaryReplicas = {replica.instId: replica
                       for node in nodes
                       for replica in node.replicas.values() if replica.isPrimary}
    return [r[1] for r in
            sorted(primaryReplicas.items(), key=operator.itemgetter(0))]


def ensureElectionsDone(looper: Looper,
                        nodes: Sequence[TestNode],
                        retryWait: float = None,  # seconds
                        customTimeout: float = 20,
                        instances_list: Sequence[int] = None,
                        check_primaries=True) -> Sequence[TestNode]:
    # TODO: Change the name to something like `ensure_primaries_selected`
    # since there might not always be an election, there might be a round
    # robin selection
    """
    Wait for elections to be complete

    :param retryWait:
    :param customTimeout: specific timeout
    :param instances_list: expected number of protocol instances
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
        instances=instances_list,
        check_primaries=check_primaries)


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
    params = [args for args in getAllArgs(node.view_changer, ViewChanger.start_view_change)]
    assert len(params) > 0
    args = params[-1]
    assert args["proposedViewNo"] == proposedViewNo
    assert node.viewNo == proposedViewNo


def timeThis(func, *args, **kwargs):
    s = time.perf_counter()
    res = func(*args, **kwargs)
    return res, time.perf_counter() - s


def instances(nodes: Sequence[Node],
              instances: Sequence[int] = None) -> Dict[int, List[replica.Replica]]:
    instances = (range(getRequiredInstances(len(nodes)))
                 if instances is None else instances)
    for n in nodes:
        assert len(n.replicas) == len(instances), "Node: {}".format(n)
    return {i: [n.replicas[i] for n in nodes] for i in instances}


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
        nm = replica_name_to_node_name(node.replicas[0].primaryName)
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

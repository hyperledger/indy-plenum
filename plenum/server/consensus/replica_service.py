from typing import List, NamedTuple

from orderedset._orderedset import OrderedSet

from crypto.bls.bls_bft_replica import BlsBftReplica
from ledger.ledger import Ledger
from plenum.common.config_util import getConfig
from plenum.common.constants import TXN_TYPE
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import NodeNeedViewChange, NeedViewChange
from plenum.common.messages.node_messages import Checkpoint, Ordered
from plenum.common.stashing_router import StashingRouter
from plenum.common.timer import TimerService
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.view_change_trigger_service import ViewChangeTriggerService
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.primary_selector import RoundRobinNodeRegPrimariesSelector
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.server.replica_freshness_checker import FreshnessChecker
from plenum.server.replica_helper import generateName
from plenum.server.request_managers.write_request_manager import WriteRequestManager


NeedAddNode = NamedTuple('NeedAddNode', [('name', str), ('who', str)])
NeedRemoveNode = NamedTuple('NeedRemoveNode', [('name', str), ('who', str)])


class ReplicaService:
    """
    This is a wrapper consensus-related services. Now it is intended mostly for
    simulation tests, however in future it can replace actual Replica in plenum.
    """

    def __init__(self, name: str, validators: List[str], primary_name: str,
                 timer: TimerService, bus: InternalBus, network: ExternalBus,
                 write_manager: WriteRequestManager,
                 bls_bft_replica: BlsBftReplica = None):
        # ToDo: Maybe ConsensusSharedData should be initiated before and passed already prepared?
        self._network = network
        self._data = ConsensusSharedData(name, validators, 0)
        self._data.primary_name = generateName(primary_name, self._data.inst_id)
        self.config = getConfig()
        self.stasher = StashingRouter(self.config.REPLICA_STASH_LIMIT, buses=[bus, network])
        self._write_manager = write_manager
        self._primaries_selector = RoundRobinNodeRegPrimariesSelector(self._write_manager.node_reg_handler)
        self._orderer = OrderingService(data=self._data,
                                        timer=timer,
                                        bus=bus,
                                        network=network,
                                        write_manager=self._write_manager,
                                        bls_bft_replica=bls_bft_replica,
                                        freshness_checker=FreshnessChecker(
                                            freshness_timeout=self.config.STATE_FRESHNESS_UPDATE_INTERVAL),
                                        primaries_selector=self._primaries_selector,
                                        stasher=self.stasher)
        self._checkpointer = CheckpointService(self._data, bus, network, self.stasher,
                                               write_manager.database_manager)
        self._view_changer = ViewChangeService(self._data, timer, bus, network, self.stasher, self._primaries_selector)
        self._view_change_trigger = ViewChangeTriggerService(data=self._data,
                                                             timer=timer,
                                                             bus=bus,
                                                             network=network,
                                                             db_manager=write_manager.database_manager,
                                                             is_master_degraded=lambda: False,
                                                             stasher=self.stasher)
        self._message_requestor = MessageReqService(self._data, bus, network)

        self._add_ledgers()

        # TODO: This is just for testing purposes only
        self._data.checkpoints.append(
            Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0,
                       digest='4F7BsTMVPKFshM1MwLf6y23cid6fL3xMpazVoF9krzUw'))

        # ToDo: it should be done in Zero-view stage.
        write_manager.on_catchup_finished()
        self._data.primaries = self._view_changer._primaries_selector.select_primaries(self._data.view_no)

        # Simulate node behavior
        self._internal_bus = bus
        self._internal_bus.subscribe(NodeNeedViewChange, self.process_node_need_view_change)
        self._internal_bus.subscribe(Ordered, self.emulate_ordered_processing)

        # ToDo: ugly way to understand node_reg changing
        self._previous_node_reg = self._write_manager.node_reg_handler.committed_node_reg

    def ready_for_3pc(self, req_key):
        fin_req = self._data.requests[req_key.digest].finalised
        req_type = fin_req.operation[TXN_TYPE]
        ledger_id = self._write_manager.type_to_ledger_id.get(req_type)
        queue = self._orderer.requestQueues[ledger_id]
        queue.add(req_key.digest)

    def emulate_ordered_processing(self, msg: Ordered):
        three_pc_batch = ThreePcBatch.from_ordered(msg)
        three_pc_batch.txn_root = Ledger.hashToStr(three_pc_batch.txn_root)
        three_pc_batch.state_root = Ledger.hashToStr(three_pc_batch.state_root)
        self._write_manager.commit_batch(three_pc_batch)

        possible_added = set(three_pc_batch.node_reg) - set(self._previous_node_reg)
        possible_removed = set(self._previous_node_reg) - set(three_pc_batch.node_reg)
        if possible_added:
            for node_name in list(possible_added):
                self._internal_bus.send(NeedAddNode(node_name, self.name))
        if possible_removed:
            for node_name in list(possible_removed):
                self._internal_bus.send(NeedRemoveNode(node_name, self.name))
        if possible_added or possible_removed:
            self._previous_node_reg = three_pc_batch.node_reg

    def _add_ledgers(self):
        for lid in self._write_manager.ledger_ids:
            self._orderer.requestQueues[lid] = OrderedSet()

    @property
    def name(self):
        return self._data.name

    def __repr__(self):
        return self.name

    def process_node_need_view_change(self, msg: NodeNeedViewChange):
        self._internal_bus.send(NeedViewChange(msg.view_no))

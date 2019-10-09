from typing import List
from orderedset._orderedset import OrderedSet

from plenum.common.constants import TXN_TYPE
from plenum.common.util import getMaxFailures
from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.server.replica_freshness_checker import FreshnessChecker

from crypto.bls.bls_bft_replica import BlsBftReplica
from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.node_messages import Checkpoint
from plenum.common.stashing_router import StashingRouter
from plenum.common.timer import TimerService
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.server.replica_helper import generateName
from plenum.server.request_managers.write_request_manager import WriteRequestManager


class ReplicaService:
    """
    This is a wrapper consensus-related services. Now it is intended mostly for
    simulation tests, however in future it can replace actual Replica in plenum.
    """

    def __init__(self, name: str, validators: List[str], primary_name: str,
                 timer: TimerService, bus: InternalBus, network: ExternalBus,
                 write_manager: WriteRequestManager,
                 bls_bft_replica: BlsBftReplica=None):
        # ToDo: Maybe ConsensusSharedData should be initiated before and passed already prepared?
        self._data = ConsensusSharedData(name, validators, 0)
        self._data.primary_name = generateName(primary_name, self._data.inst_id)
        self.config = getConfig()
        self.stasher = StashingRouter(self.config.REPLICA_STASH_LIMIT, buses=[bus, network])
        self._write_manager = write_manager
        self._orderer = OrderingService(data=self._data,
                                        timer=timer,
                                        bus=bus,
                                        network=network,
                                        write_manager=self._write_manager,
                                        bls_bft_replica=bls_bft_replica,
                                        freshness_checker=FreshnessChecker(
                                            freshness_timeout=self.config.STATE_FRESHNESS_UPDATE_INTERVAL),
                                        stasher=self.stasher)
        self._orderer._validator = OrderingServiceMsgValidator(self._orderer._data)
        self._checkpointer = CheckpointService(self._data, bus, network, self.stasher,
                                               write_manager.database_manager)
        self._view_changer = ViewChangeService(self._data, timer, bus, network, self.stasher)
        self._message_requestor = MessageReqService(self._data, bus, network)

        self._add_ledgers()

        # TODO: This is just for testing purposes only
        self._data.checkpoints.append(
            Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0,
                       digest='4F7BsTMVPKFshM1MwLf6y23cid6fL3xMpazVoF9krzUw'))

        # ToDo: it should be done in Zero-view stage.
        self._data.primaries = self._view_changer._primaries_selector.select_primaries(self._data.view_no,
                                                                                       getMaxFailures(len(validators)) + 1,
                                                                                       validators)

    def ready_for_3pc(self, req_key):
        fin_req = self._data.requests[req_key.digest].finalised
        req_type = fin_req.operation[TXN_TYPE]
        ledger_id = self._write_manager.type_to_ledger_id.get(req_type)
        queue = self._orderer.requestQueues[ledger_id]
        queue.add(req_key.digest)

    def _add_ledgers(self):
        for lid in self._write_manager.ledger_ids:
            self._orderer.requestQueues[lid] = OrderedSet()

    @property
    def name(self):
        return self._data.name

    def __repr__(self):
        return self.name

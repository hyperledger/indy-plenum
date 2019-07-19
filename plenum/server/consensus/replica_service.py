from typing import List

from crypto.bls.bls_bft_replica import BlsBftReplica
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.node_messages import Checkpoint
from plenum.common.timer import TimerService
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.server.request_managers.write_request_manager import WriteRequestManager


class ReplicaService:
    """
    This is a wrapper consensus-related services. Now it is intended mostly for
    simulation tests, however in future it can replace actual Replica in plenum.
    """

    def __init__(self, name: str, validators: List[str], primary_name: str,
                 timer: TimerService, bus: InternalBus, network: ExternalBus, write_manager: WriteRequestManager=None,
                 bls_bft_replica: BlsBftReplica=None):
        self._data = ConsensusSharedData(name, validators, 0)
        self._data.primary_name = primary_name
        self._orderer = OrderingService(data=self._data,
                                        timer=timer,
                                        bus=bus,
                                        network=network,
                                        write_manager=write_manager,
                                        bls_bft_replica=bls_bft_replica)
        self._checkpointer = CheckpointService(self._data, bus, network)
        self._view_changer = ViewChangeService(self._data, timer, bus, network)

        # TODO: This is just for testing purposes only
        self._data.checkpoints.append(
            Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=0, digest='empty'))

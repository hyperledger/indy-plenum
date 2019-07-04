from typing import List

from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.timer import TimerService
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.view_change_service import ViewChangeService


class ReplicaService:
    """
    This is a wrapper consensus-related services. Now it is intended mostly for
    simulation tests, however in future it can replace actual Replica in plenum.
    """
    def __init__(self, name: str, validators: List[str], primary_name: str,
                 timer: TimerService, bus: InternalBus, network: ExternalBus):
        self._data = ConsensusDataProvider(name, validators, primary_name)
        self._orderer = OrderingService(self._data, bus, network)
        self._checkpointer = CheckpointService(self._data, bus, network)
        self._view_changer = ViewChangeService(self._data, timer, bus, network)

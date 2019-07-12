from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData


class CheckpointService:
    def __init__(self, data: ConsensusSharedData, bus: InternalBus, network: ExternalBus):
        self._data = data
        self._bus = bus
        self._network = network

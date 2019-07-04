from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider


class CheckpointService:
    def __init__(self, data: ConsensusDataProvider, bus: InternalBus, network: ExternalBus):
        self._data = data
        self._bus = bus
        self._network = network

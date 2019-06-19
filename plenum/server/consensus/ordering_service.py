from plenum.common.event_bus import ExternalBus
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider


class OrderingService:
    def __init__(self, data: ConsensusDataProvider, network: ExternalBus):
        self._data = data
        self._network = network

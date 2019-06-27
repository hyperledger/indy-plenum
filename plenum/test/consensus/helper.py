from typing import Optional, List

from plenum.common.event_bus import ExternalBus
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom


class ReplicaService:
    # TODO: To be replaced by real replica (or to replace real replica, whatever is simpler)
    def __init__(self, name: str, network: ExternalBus):
        self._data = ConsensusDataProvider(name)
        self._orderer = OrderingService(self._data, network)
        self._checkpointer = CheckpointService(self._data, network)
        self._view_changer = ViewChangeService(self._data, network)


class SimPool:
    def __init__(self, node_count: int = 4, random: Optional[SimRandom] = None):
        self._random = random if random else DefaultSimRandom()
        self._timer = MockTimer()
        self._network = SimNetwork(self._timer, self._random)
        self._nodes = [ReplicaService(name, self.network.create_peer(name))
                       for name in genNodeNames(node_count)]

    @property
    def timer(self) -> MockTimer:
        return self._timer

    @property
    def network(self) -> SimNetwork:
        return self._network

    @property
    def nodes(self) -> List[ReplicaService]:
        return self._nodes

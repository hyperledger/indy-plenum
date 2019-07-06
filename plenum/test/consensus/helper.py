from typing import Optional, List

from plenum.common.event_bus import InternalBus
from plenum.common.messages.node_messages import ViewChange
from plenum.server.consensus.replica_service import ReplicaService
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom


class SimPool:
    def __init__(self, node_count: int = 4, random: Optional[SimRandom] = None):
        self._random = random if random else DefaultSimRandom()
        self._timer = MockTimer()
        self._network = SimNetwork(self._timer, self._random)
        validators = genNodeNames(node_count)
        primary_name = validators[0]
        self._nodes = [ReplicaService(name, validators, primary_name,
                                      self._timer, InternalBus(), self.network.create_peer(name))
                       for name in validators]

    @property
    def timer(self) -> MockTimer:
        return self._timer

    @property
    def network(self) -> SimNetwork:
        return self._network

    @property
    def nodes(self) -> List[ReplicaService]:
        return self._nodes

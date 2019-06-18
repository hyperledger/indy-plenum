from plenum.common.network_service import NetworkService
from plenum.server.consensus.three_pc_state import ThreePCState


class CheckpointService:
    def __init__(self, state: ThreePCState, network: NetworkService):
        self._state = state
        self._network = network

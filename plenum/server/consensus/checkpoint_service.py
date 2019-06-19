from plenum.common.external_bus import ExternalBus
from plenum.server.consensus.three_pc_state import ThreePCState


class CheckpointService:
    def __init__(self, state: ThreePCState, network: ExternalBus):
        self._state = state
        self._network = network

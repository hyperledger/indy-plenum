from plenum.common.network_service import NetworkService
from plenum.server.consensus.three_pc_state import ThreePCState


class ViewChangeService:
    def __init__(self, state: ThreePCState, network: NetworkService):
        self._state = state
        self._network = network

    def start_view_change(self):
        # TODO: Calculate P and Q
        self._state.enter_next_view()
        # TODO: Replace with actual message
        self._network.send("ViewChange")

    def process_view_change_message(self, msg, frm: str):
        pass

    def process_view_change_ack_message(self, msg, frm: str):
        pass

    def process_new_view_message(self, msg, frm: str):
        pass

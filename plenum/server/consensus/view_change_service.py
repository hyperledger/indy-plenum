from plenum.common.channel import Router
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView
from plenum.common.network_service import NetworkService
from plenum.server.consensus.three_pc_state import ThreePCState


class ViewChangeService:
    def __init__(self, state: ThreePCState, network: NetworkService):
        self._state = state
        self._network = network

        router = Router(network.on_message())
        router.add(ViewChange, self.process_view_change_message)
        router.add(ViewChangeAck, self.process_view_change_ack_message)
        router.add(NewView, self.process_new_view_message)

    def start_view_change(self):
        # TODO: Calculate
        prepared = []
        preprepared = []

        self._state.enter_next_view()

        vc = ViewChange(
            viewNo=self._state.view_no,
            stableCheckpoint=self._state.stable_checkpoint,
            prepared=prepared,
            preprepared=preprepared,
            checkpoints=self._state.checkpoints
        )
        self._network.send(vc)

    def process_view_change_message(self, msg: ViewChange, frm: str):
        pass

    def process_view_change_ack_message(self, msg: ViewChangeAck, frm: str):
        pass

    def process_new_view_message(self, msg: NewView, frm: str):
        pass

from plenum.common.external_bus import ExternalBus
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView
from plenum.server.consensus.three_pc_state import ThreePCState


class ViewChangeService:
    def __init__(self, state: ThreePCState, network: ExternalBus):
        self._state = state
        self._network = network

        network.subscribe(ViewChange, self.process_view_change_message)
        network.subscribe(ViewChangeAck, self.process_view_change_ack_message)
        network.subscribe(NewView, self.process_new_view_message)

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
        # TODO: Validation

        vca = ViewChangeAck(
            viewNo=msg.viewNo,
            name=frm,
            digest='digest_of_view_change_message'
        )
        self._network.send(vca, self._state.primary_name)

    def process_view_change_ack_message(self, msg: ViewChangeAck, frm: str):
        pass

    def process_new_view_message(self, msg: NewView, frm: str):
        pass

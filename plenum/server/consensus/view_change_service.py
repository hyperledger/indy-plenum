from plenum.common.event_bus import ExternalBus
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider


class ViewChangeService:
    def __init__(self, data: ConsensusDataProvider, network: ExternalBus):
        self._data = data
        self._network = network

        network.subscribe(ViewChange, self.process_view_change_message)
        network.subscribe(ViewChangeAck, self.process_view_change_ack_message)
        network.subscribe(NewView, self.process_new_view_message)

    def start_view_change(self):
        # TODO: Calculate
        prepared = []
        preprepared = []

        self._data.view_no += 1
        self._data.waiting_for_new_view = True

        vc = ViewChange(
            viewNo=self._data.view_no,
            stableCheckpoint=self._data.stable_checkpoint,
            prepared=prepared,
            preprepared=preprepared,
            checkpoints=self._data.checkpoints
        )
        self._network.send(vc)

    def process_view_change_message(self, msg: ViewChange, frm: str):
        # TODO: Validation

        vca = ViewChangeAck(
            viewNo=msg.viewNo,
            name=frm,
            digest='digest_of_view_change_message'
        )
        self._network.send(vca, self._data.primary_name)

    def process_view_change_ack_message(self, msg: ViewChangeAck, frm: str):
        # TODO: Validation

        nv = NewView(
            viewNo=msg.viewNo,
            viewChanges=[],
            checkpoint=None,
            preprepares=[]
        )
        self._network.send(nv)
        self._data.waiting_for_new_view = False

    def process_new_view_message(self, msg: NewView, frm: str):
        # TODO: Validation

        self._data.waiting_for_new_view = False

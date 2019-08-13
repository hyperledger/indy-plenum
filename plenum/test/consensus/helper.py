from typing import Dict, Type

from plenum.common.messages.node_messages import Checkpoint, ViewChange, NewView
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData, BatchID
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.view_change_service import ViewChangeService

VIEW_CHANGE_SERVICE_FIELDS = 'view_no', 'waiting_for_new_view', 'primaries'
ORDERING_SERVICE_FIELDS = 'last_ordered_3pc', 'preprepared', 'prepared'
CHECKPOINT_SERVICE_FIELDS = 'stable_checkpoint', 'checkpoints', 'low_watermark', 'high_watermark'

FIELDS = {ViewChangeService: VIEW_CHANGE_SERVICE_FIELDS,
          OrderingService: ORDERING_SERVICE_FIELDS,
          CheckpointService: CHECKPOINT_SERVICE_FIELDS}


def copy_shared_data(data: ConsensusSharedData) -> Dict:
    fields_to_check = VIEW_CHANGE_SERVICE_FIELDS + ORDERING_SERVICE_FIELDS + CHECKPOINT_SERVICE_FIELDS
    data_vars = vars(data)
    return {k: data_vars[k] for k in fields_to_check}


def check_service_changed_only_owned_fields_in_shared_data(service: Type,
                                                           data1: Dict, data2: Dict):
    changed_field = FIELDS[service]
    data1 = {k: v for k, v in data1.items() if k not in changed_field}
    data2 = {k: v for k, v in data2.items() if k not in changed_field}
    assert data1 == data2


def create_checkpoints(view_no):
    return [Checkpoint(instId=0, viewNo=view_no, seqNoStart=0, seqNoEnd=200, digest='some')]


def create_view_changes(initial_view_no):
    return [ViewChange(viewNo=initial_view_no + 1,
                       stableCheckpoint=200,
                       prepared=[BatchID(initial_view_no, 10, "d1"), BatchID(initial_view_no, 11, "d1")],
                       preprepared=[BatchID(initial_view_no, 10, "d1")],
                       checkpoints=[
                           Checkpoint(instId=0, viewNo=initial_view_no, seqNoStart=200, seqNoEnd=300, digest='some')],
                       ),
            ViewChange(viewNo=initial_view_no + 1,
                       stableCheckpoint=200,
                       prepared=[BatchID(initial_view_no, 10, "d1")],
                       preprepared=[],
                       checkpoints=[
                           Checkpoint(instId=0, viewNo=initial_view_no, seqNoStart=0, seqNoEnd=200, digest='some')]
                       )
            ]


def create_batches(view_no):
    return [BatchID(view_no, 10, "d1"),
            BatchID(view_no, 11, "d2"),
            BatchID(view_no, 12, "d3")]


def create_new_view(initial_view_no, stable_cp):
    vcs = create_view_changes(initial_view_no)
    batches = create_batches(initial_view_no)
    cp = Checkpoint(instId=0, viewNo=initial_view_no, seqNoStart=0, seqNoEnd=stable_cp, digest='some')
    return NewView(initial_view_no + 1, vcs, cp, batches)

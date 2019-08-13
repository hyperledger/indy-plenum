from operator import itemgetter
from typing import Dict, Type

from plenum.common.messages.node_messages import Checkpoint, ViewChange, NewView, ViewChangeAck
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData, BatchID
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector
from plenum.server.consensus.view_change_service import ViewChangeService, view_change_digest
from plenum.test.greek import genNodeNames

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


def create_batches(view_no):
    return [BatchID(view_no, 11, "d1"),
            BatchID(view_no, 12, "d2"),
            BatchID(view_no, 13, "d3")]


def create_view_change(initial_view_no, stable_cp=10, batches=None):
    if batches is None:
        batches = create_batches(initial_view_no)
    cp = Checkpoint(instId=0, viewNo=initial_view_no, seqNoStart=0, seqNoEnd=stable_cp, digest='some')
    return ViewChange(viewNo=initial_view_no + 1,
                      stableCheckpoint=stable_cp,
                      prepared=batches,
                      preprepared=batches,
                      checkpoints=[cp])


def create_new_view_from_vc(vc, validators, checkpoint=None, batches=None):
    vc_digest = view_change_digest(vc)
    vcs = [(node_name, vc_digest) for node_name in validators]
    checkpoint = checkpoint or vc.checkpoints[0]
    batches = batches or vc.prepared
    return NewView(vc.viewNo,
                   sorted(vcs, key=itemgetter(0)),
                   checkpoint,
                   batches)


def create_new_view(initial_view_no, stable_cp, validators=None):
    validators = validators or genNodeNames(4)
    batches = create_batches(initial_view_no)
    vc = create_view_change(initial_view_no, stable_cp, batches)
    return create_new_view_from_vc(vc, validators)


def create_view_change_acks(vc, vc_frm, senders):
    digest = view_change_digest(vc)
    senders = [name for name in senders if name != vc_frm]
    return [(ViewChangeAck(viewNo=vc.viewNo, name=vc_frm, digest=digest), ack_frm) for ack_frm in senders]

def primary_in_view(validators, view_no):
    f = (len(validators) - 1) // 3
    return RoundRobinPrimariesSelector().select_primaries(view_no=view_no, instance_count=f + 1,
                                                          validators=validators)[0]
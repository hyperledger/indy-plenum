from plenum.common.messages.internal_messages import NeedViewChange, NewViewCheckpointsApplied
from plenum.common.startable import Mode
from plenum.server.replica_validator_enums import STASH_VIEW_3PC, STASH_WAITING_VIEW_CHANGE
from plenum.test.consensus.helper import create_new_view
from plenum.test.helper import create_commit_no_bls_sig


def test_unstash_future_view_on_need_view_change(external_bus, internal_bus,
                                                 replica_service, validators):
    replica_service._data.view_no = 1
    replica_service._data.node_mode = Mode.participating
    external_bus.process_incoming(create_new_view(initial_view_no=1, stable_cp=200),
                                  replica_service._data.primary_name)
    external_bus.process_incoming(create_commit_no_bls_sig(req_key=(2, 10)),
                                  replica_service._data.primary_name)
    assert replica_service.stasher.stash_size(STASH_WAITING_VIEW_CHANGE) == 1
    assert replica_service.stasher.stash_size(STASH_VIEW_3PC) == 1

    internal_bus.send(NeedViewChange(view_no=2))

    assert replica_service.stasher.stash_size(STASH_WAITING_VIEW_CHANGE) == 0
    assert replica_service.stasher.stash_size(STASH_VIEW_3PC) == 1


def test_unstash_waiting_new_view_on_new_view_checkpoint_applied(external_bus, internal_bus,
                                                                 replica_service, validators):
    replica_service._data.view_no = 2
    replica_service._data.node_mode = Mode.participating
    replica_service._data.waiting_for_new_view = True
    replica_service._data.prev_view_prepare_cert = 5
    new_view = create_new_view(initial_view_no=1, stable_cp=200, batches=[])

    external_bus.process_incoming(create_commit_no_bls_sig(req_key=(2, 10)),
                                  replica_service._data.primary_name)
    assert replica_service.stasher.stash_size(STASH_VIEW_3PC) == 1

    replica_service._data.waiting_for_new_view = False
    internal_bus.send(NewViewCheckpointsApplied(view_no=2,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    assert replica_service.stasher.stash_size(STASH_VIEW_3PC) == 0

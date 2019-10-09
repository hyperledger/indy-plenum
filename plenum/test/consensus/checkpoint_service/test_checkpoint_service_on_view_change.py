import pytest
from unittest.mock import Mock

from plenum.common.messages.internal_messages import ViewChangeStarted, NewViewAccepted, NewViewCheckpointsApplied
from plenum.common.messages.node_messages import Checkpoint
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.test.checkpoints.helper import cp_digest
from plenum.test.consensus.helper import copy_shared_data, check_service_changed_only_owned_fields_in_shared_data, \
    create_new_view, create_checkpoints


def test_do_nothing_on_view_change_started(internal_bus, checkpoint_service):
    checkpoint_service._data.checkpoints.clear()
    checkpoint_service._data.checkpoints.update(create_checkpoints(view_no=0))
    checkpoint_service._data.stable_checkpoint = 200
    checkpoint_service._data.low_watermark = 200
    checkpoint_service._data.high_watermark = checkpoint_service._data.low_watermark + 300
    old_data = copy_shared_data(checkpoint_service._data)

    internal_bus.send(ViewChangeStarted(view_no=4))

    new_data = copy_shared_data(checkpoint_service._data)
    assert old_data == new_data


@pytest.mark.parametrize('checkpoints, stable_checkpoint, checkpoints_result, stable_cp_result', [
    ([Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=200, digest=cp_digest(200))],
     100,
     [Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=200, digest=cp_digest(200))],
     200),

    ([Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=0, digest=cp_digest(0))],
     0,
     [Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=0, digest=cp_digest(0))],
     0),

    ([Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=100, digest=cp_digest(100))],
     0,
     [Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=100, digest=cp_digest(100))],
     0),

    ([Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=100, digest=cp_digest(100))],
     100,
     [Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=100, digest=cp_digest(100))],
     100),

    ([Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=100, digest=cp_digest(100)),
      Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=200, digest=cp_digest(200))],
     100,
     [Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=200, digest=cp_digest(200))],
     200),

    ([Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=100, digest=cp_digest(100)),
      Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=200, digest=cp_digest(200)),
      Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=300, digest=cp_digest(300)),
      Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=400, digest=cp_digest(400))],
     100,
     [Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=200, digest=cp_digest(200)),
      Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=300, digest=cp_digest(300)),
      Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=400, digest=cp_digest(400))],
     200),

])
def test_update_shared_data_on_new_view_accepted(internal_bus, checkpoint_service,
                                                 checkpoints, stable_checkpoint,
                                                 checkpoints_result, stable_cp_result, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    old_data = copy_shared_data(checkpoint_service._data)
    checkpoint_service._data.checkpoints.clear()
    checkpoint_service._data.checkpoints.update(checkpoints)
    checkpoint_service._data.stable_checkpoint = stable_checkpoint
    checkpoint_service._data.low_watermark = stable_checkpoint
    checkpoint_service._data.high_watermark = checkpoint_service._data.low_watermark + 300

    initial_view_no = 3
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200)
    internal_bus.send(NewViewAccepted(view_no=initial_view_no + 1,
                                      view_changes=new_view.viewChanges,
                                      checkpoint=new_view.checkpoint,
                                      batches=new_view.batches))

    new_data = copy_shared_data(checkpoint_service._data)
    check_service_changed_only_owned_fields_in_shared_data(CheckpointService, old_data, new_data)

    assert list(checkpoint_service._data.checkpoints) == checkpoints_result
    assert checkpoint_service._data.stable_checkpoint == stable_cp_result
    assert checkpoint_service._data.low_watermark == stable_cp_result
    assert checkpoint_service._data.high_watermark == checkpoint_service._data.low_watermark + 300


def test_do_nothing_on_new_view_checkpoint_applied(internal_bus, checkpoint_service):
    checkpoint_service._data.checkpoints.clear()
    checkpoint_service._data.checkpoints.update(create_checkpoints(view_no=0))
    checkpoint_service._data.stable_checkpoint = 100
    checkpoint_service._data.low_watermark = 100
    checkpoint_service._data.high_watermark = checkpoint_service._data.low_watermark + 300
    old_data = copy_shared_data(checkpoint_service._data)

    initial_view_no = 3
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200)
    internal_bus.send(NewViewCheckpointsApplied(view_no=initial_view_no + 1,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    new_data = copy_shared_data(checkpoint_service._data)
    assert old_data == new_data


def test_view_change_finished_sends_new_view_checkpoint_applied(internal_bus, checkpoint_service, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    handler = Mock()
    internal_bus.subscribe(NewViewCheckpointsApplied, handler)

    initial_view_no = 3
    checkpoint_service._data.checkpoints.clear()
    checkpoint_service._data.checkpoints.update(
        [Checkpoint(instId=0, viewNo=3, seqNoStart=0, seqNoEnd=200, digest=cp_digest(200))])
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200)
    internal_bus.send(NewViewAccepted(view_no=initial_view_no + 1,
                                      view_changes=new_view.viewChanges,
                                      checkpoint=new_view.checkpoint,
                                      batches=new_view.batches))
    expected_apply_new_view = NewViewCheckpointsApplied(view_no=initial_view_no + 1,
                                                        view_changes=new_view.viewChanges,
                                                        checkpoint=new_view.checkpoint,
                                                        batches=new_view.batches)

    handler.assert_called_once_with(expected_apply_new_view)

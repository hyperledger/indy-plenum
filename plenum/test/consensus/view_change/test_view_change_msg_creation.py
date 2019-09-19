import string

import pytest

from plenum.common.messages.internal_messages import NeedViewChange
from plenum.common.messages.node_messages import ViewChange, Checkpoint
from plenum.server.consensus.batch_id import BatchID
from plenum.server.consensus.view_change_storages import view_change_digest
from plenum.test.checkpoints.helper import cp_digest
from plenum.test.consensus.helper import create_view_change, create_batches


@pytest.fixture
def data(view_change_service):
    return view_change_service._data


def get_view_change(view_change_service):
    assert len(view_change_service._network.sent_messages) > 0
    msg, _ = view_change_service._network.sent_messages[-1]
    assert isinstance(msg, ViewChange)
    return msg


def test_view_change_data(view_change_service, data):
    data.view_no = 1
    data.checkpoints.clear()
    cp = Checkpoint(instId=0, viewNo=1, seqNoStart=0, seqNoEnd=10, digest=cp_digest(10))
    data.checkpoints.add(cp)
    data.stable_checkpoint = 10
    data.prepared = [BatchID(0, 0, 1, "digest1"),
                     BatchID(0, 0, 2, "digest2")]
    data.preprepared = [BatchID(0, 0, 1, "digest1"),
                        BatchID(0, 0, 2, "digest2"),
                        BatchID(0, 0, 3, "digest3")]

    view_change_service._bus.send(NeedViewChange())

    assert data.view_no == 2
    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2")]
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"), (0, 0, 3, "digest3")]
    assert msg.stableCheckpoint == 10
    assert msg.checkpoints == [cp]


def test_view_change_data_multiple(view_change_service, data):
    # view 0 -> 1
    data.view_no = 0
    cp1 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest=cp_digest(10))
    data.checkpoints.add(cp1)
    data.stable_checkpoint = 0
    data.prepared = [BatchID(0, 0, 1, "digest1"),
                     BatchID(0, 0, 2, "digest2")]
    data.preprepared = [BatchID(0, 0, 1, "digest1"),
                        BatchID(0, 0, 2, "digest2"),
                        BatchID(0, 0, 3, "digest3")]

    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 1

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.prepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2")]
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"), (0, 0, 3, "digest3")]
    assert msg.stableCheckpoint == 0
    assert msg.checkpoints == [data.initial_checkpoint, cp1]

    # view 1 -> 2
    data.view_no = 1
    cp2 = Checkpoint(instId=0, viewNo=1, seqNoStart=0, seqNoEnd=20, digest=cp_digest(20))
    data.checkpoints.add(cp2)
    data.stable_checkpoint = 0
    data.prepared = [BatchID(1, 1, 11, "digest11"),
                     BatchID(1, 1, 12, "digest12")]
    data.preprepared = [BatchID(1, 1, 11, "digest11"),
                        BatchID(1, 1, 12, "digest12"),
                        BatchID(1, 1, 13, "digest13")]

    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"),
                            (1, 1, 11, "digest11"), (1, 1, 12, "digest12")]
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"), (0, 0, 3, "digest3"),
                               (1, 1, 11, "digest11"), (1, 1, 12, "digest12"), (1, 1, 13, "digest13")]
    assert msg.stableCheckpoint == 0
    assert msg.checkpoints == [data.initial_checkpoint, cp1, cp2]


def test_view_change_data_multiple_respects_checkpoint(view_change_service, data):
    # view 0 -> 1
    data.view_no = 0
    cp1 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest=cp_digest(10))
    data.checkpoints.add(cp1)
    data.stable_checkpoint = 0
    data.prepared = [BatchID(0, 0, 1, "digest1"),
                     BatchID(0, 0, 2, "digest2")]
    data.preprepared = [BatchID(0, 0, 1, "digest1"),
                        BatchID(0, 0, 2, "digest2"),
                        BatchID(0, 0, 3, "digest3")]

    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 1

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.prepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2")]
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"), (0, 0, 3, "digest3")]
    assert msg.stableCheckpoint == 0
    assert msg.checkpoints == [data.initial_checkpoint, cp1]

    # view 1 -> 2
    data.view_no = 1
    cp2 = Checkpoint(instId=0, viewNo=1, seqNoStart=0, seqNoEnd=20, digest=cp_digest(20))
    data.checkpoints.add(cp2)
    data.stable_checkpoint = 10
    # Here we simulate checkpoint stabilization logic of CheckpointService, which
    # clears all checkpoints below stabilized one
    data.checkpoints.remove(data.initial_checkpoint)
    data.prepared = [BatchID(1, 1, 11, "digest11"),
                     BatchID(1, 1, 12, "digest12")]
    data.preprepared = [BatchID(1, 1, 11, "digest11"),
                        BatchID(1, 1, 12, "digest12"),
                        BatchID(1, 1, 13, "digest13")]

    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(1, 1, 11, "digest11"), (1, 1, 12, "digest12")]
    assert msg.preprepared == [(1, 1, 11, "digest11"), (1, 1, 12, "digest12"), (1, 1, 13, "digest13")]
    assert msg.stableCheckpoint == 10
    assert msg.checkpoints == [cp1, cp2]


def test_view_change_empty_prepares(view_change_service, data):
    data.prepared = []
    data.preprepared = []

    view_change_service._bus.send(NeedViewChange())

    msg = get_view_change(view_change_service)
    assert msg.prepared == []
    assert msg.preprepared == []


def test_view_change_replaces_prepare(view_change_service, data):
    data.view_no = 0
    data.prepared = [BatchID(0, 0, 1, "digest1"),
                     BatchID(0, 0, 2, "digest2")]

    # view no 0->1
    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 1

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.prepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2")]

    # view no 1->2
    # replace by different viewNo and digest
    data.prepared = [BatchID(1, 1, 1, "digest11"),
                     BatchID(1, 1, 2, "digest22")]
    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(1, 1, 1, "digest11"), (1, 1, 2, "digest22")]

    # view no 2->3
    # replace by different viewNo only
    data.prepared = [BatchID(2, 2, 2, "digest22"),
                     BatchID(2, 2, 3, "digest3")]
    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 3

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 3
    assert msg.prepared == [(1, 1, 1, "digest11"), (2, 2, 2, "digest22"), (2, 2, 3, "digest3")]


def test_view_change_replaces_prepare_diff_pp_view_no(view_change_service, data):
    data.view_no = 0
    data.prepared = [BatchID(0, 0, 1, "digest1"),
                     BatchID(0, 0, 2, "digest2")]

    # view no 0->1
    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 1

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.prepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2")]

    # view no 1->2
    # replace by different viewNo and digest
    data.prepared = [BatchID(1, 0, 1, "digest11"),
                     BatchID(1, 0, 2, "digest22")]
    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(1, 0, 1, "digest11"), (1, 0, 2, "digest22")]

    # view no 2->3
    # replace by different viewNo only
    data.prepared = [BatchID(2, 2, 2, "digest22"),
                     BatchID(1, 1, 1, "digest11"),
                     BatchID(2, 2, 3, "digest3")]
    view_change_service._bus.send(NeedViewChange())
    assert data.view_no == 3

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 3
    assert msg.prepared == [(1, 1, 1, "digest11"), (2, 2, 2, "digest22"), (2, 2, 3, "digest3")]


def test_view_change_keeps_preprepare(view_change_service, data):
    data.view_no = 0
    data.preprepared = [BatchID(0, 0, 1, "digest1"),
                        BatchID(0, 0, 2, "digest2")]

    # view no 0->1
    view_change_service._bus.send(NeedViewChange())

    assert data.view_no == 1

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2")]

    # view no 1->2
    # do not replace since different viewNo and digest
    data.preprepared = [BatchID(1, 1, 1, "digest11"),
                        BatchID(1, 1, 2, "digest22")]
    view_change_service._bus.send(NeedViewChange())

    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"),
                               (1, 1, 1, "digest11"), (1, 1, 2, "digest22")]

    # view no 2->3
    #  replace by different viewNo only
    data.preprepared = [BatchID(2, 2, 2, "digest22"),
                        BatchID(2, 2, 3, "digest3")]
    view_change_service._bus.send(NeedViewChange())

    assert data.view_no == 3

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 3
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"),
                               (1, 1, 1, "digest11"),
                               (2, 2, 2, "digest22"), (2, 2, 3, "digest3")]


def test_view_change_keeps_preprepare_diff_pp_view_no(view_change_service, data):
    data.view_no = 0
    data.preprepared = [BatchID(0, 0, 1, "digest1"),
                        BatchID(0, 0, 2, "digest2")]

    # view no 0->1
    view_change_service._bus.send(NeedViewChange())

    assert data.view_no == 1

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2")]

    # view no 1->2
    # do not replace since different viewNo and digest
    data.preprepared = [BatchID(1, 0, 1, "digest11"),
                        BatchID(1, 0, 2, "digest22")]
    view_change_service._bus.send(NeedViewChange())

    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"),
                               (1, 0, 1, "digest11"), (1, 0, 2, "digest22")]

    # view no 2->3
    #  replace by different viewNo only
    data.preprepared = [BatchID(1, 1, 1, "digest11"),
                        BatchID(2, 0, 2, "digest22"),
                        BatchID(2, 2, 3, "digest3")]
    view_change_service._bus.send(NeedViewChange())

    assert data.view_no == 3

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 3
    assert msg.preprepared == [(0, 0, 1, "digest1"), (0, 0, 2, "digest2"),
                               (1, 1, 1, "digest11"),
                               (2, 0, 2, "digest22"), (2, 2, 3, "digest3")]


def test_different_view_change_messages_have_different_digests(random):
    batches = create_batches(view_no=0)
    assert view_change_digest(create_view_change(initial_view_no=0, stable_cp=100, batches=batches)) != \
           view_change_digest(create_view_change(initial_view_no=1, stable_cp=100, batches=batches))
    assert view_change_digest(create_view_change(initial_view_no=1, stable_cp=100, batches=batches)) != \
           view_change_digest(create_view_change(initial_view_no=1, stable_cp=101, batches=batches))
    assert view_change_digest(
        create_view_change(initial_view_no=1, stable_cp=100, batches=create_batches(view_no=0))) != \
           view_change_digest(create_view_change(initial_view_no=1, stable_cp=100, batches=create_batches(view_no=1)))
    assert view_change_digest(create_view_change(initial_view_no=0, stable_cp=100, batches=batches)) == \
           view_change_digest(create_view_change(initial_view_no=0, stable_cp=100, batches=batches))


def test_view_change_digest_is_256_bit_hexdigest(random):
    digest = view_change_digest(
        create_view_change(initial_view_no=0, stable_cp=random.integer(0, 10000), batches=create_batches(view_no=0)))
    assert isinstance(digest, str)
    assert len(digest) == 64
    assert all(v in string.hexdigits for v in digest)

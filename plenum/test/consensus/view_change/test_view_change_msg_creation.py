import string

import pytest

from plenum.common.messages.node_messages import ViewChange, Checkpoint
from plenum.server.consensus.view_change_service import view_change_digest
from plenum.test.consensus.view_change.helper import some_preprepare


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
    cp = Checkpoint(instId=0, viewNo=1, seqNoStart=0, seqNoEnd=10, digest='empty')
    data.checkpoints.add(cp)
    data.stable_checkpoint = 10
    data.prepared = [some_preprepare(0, 1, "digest1"),
                     some_preprepare(0, 2, "digest2")]
    data.preprepared = [some_preprepare(0, 1, "digest1"),
                        some_preprepare(0, 2, "digest2"),
                        some_preprepare(0, 3, "digest3")]
    view_change_service.start_view_change()

    assert data.prepared == []
    assert data.preprepared == []
    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(0, 1, "digest1"), (0, 2, "digest2")]
    assert msg.preprepared == [(0, 1, "digest1"), (0, 2, "digest2"), (0, 3, "digest3")]
    assert msg.stableCheckpoint == 10
    assert msg.checkpoints == [cp]


def test_view_change_data_multiple(view_change_service, data):
    # view 0 -> 1
    data.view_no = 0
    cp1 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='empty')
    data.checkpoints.add(cp1)
    data.stable_checkpoint = 0
    data.prepared = [some_preprepare(0, 1, "digest1"),
                     some_preprepare(0, 2, "digest2")]
    data.preprepared = [some_preprepare(0, 1, "digest1"),
                        some_preprepare(0, 2, "digest2"),
                        some_preprepare(0, 3, "digest3")]
    view_change_service.start_view_change()

    assert data.prepared == []
    assert data.preprepared == []
    assert data.view_no == 1

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.prepared == [(0, 1, "digest1"), (0, 2, "digest2")]
    assert msg.preprepared == [(0, 1, "digest1"), (0, 2, "digest2"), (0, 3, "digest3")]
    assert msg.stableCheckpoint == 0
    assert msg.checkpoints == [cp1]

    # view 1 -> 2
    data.view_no = 1
    cp2 = Checkpoint(instId=0, viewNo=1, seqNoStart=10, seqNoEnd=20, digest='empty')
    data.checkpoints.add(cp2)
    data.stable_checkpoint = 0
    data.prepared = [some_preprepare(1, 11, "digest11"),
                     some_preprepare(1, 12, "digest12")]
    data.preprepared = [some_preprepare(1, 11, "digest11"),
                        some_preprepare(1, 12, "digest12"),
                        some_preprepare(1, 13, "digest13")]
    view_change_service.start_view_change()

    assert data.prepared == []
    assert data.preprepared == []
    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(0, 1, "digest1"), (0, 2, "digest2"),
                            (1, 11, "digest11"), (1, 12, "digest12")]
    assert msg.preprepared == [(0, 1, "digest1"), (0, 2, "digest2"), (0, 3, "digest3"),
                               (1, 11, "digest11"), (1, 12, "digest12"), (1, 13, "digest13")]
    assert msg.stableCheckpoint == 0
    assert msg.checkpoints == [cp1, cp2]


def test_view_change_data_multiple_respects_checkpoint(view_change_service, data):
    # view 0 -> 1
    data.view_no = 0
    cp1 = Checkpoint(instId=0, viewNo=0, seqNoStart=0, seqNoEnd=10, digest='empty')
    data.checkpoints.add(cp1)
    data.stable_checkpoint = 0
    data.prepared = [some_preprepare(0, 1, "digest1"),
                     some_preprepare(0, 2, "digest2")]
    data.preprepared = [some_preprepare(0, 1, "digest1"),
                        some_preprepare(0, 2, "digest2"),
                        some_preprepare(0, 3, "digest3")]
    view_change_service.start_view_change()

    assert data.prepared == []
    assert data.preprepared == []
    assert data.view_no == 1

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.prepared == [(0, 1, "digest1"), (0, 2, "digest2")]
    assert msg.preprepared == [(0, 1, "digest1"), (0, 2, "digest2"), (0, 3, "digest3")]
    assert msg.stableCheckpoint == 0
    assert msg.checkpoints == [cp1]

    # view 1 -> 2
    data.view_no = 1
    cp2 = Checkpoint(instId=0, viewNo=1, seqNoStart=10, seqNoEnd=20, digest='empty')
    data.checkpoints.add(cp2)
    data.stable_checkpoint = 10
    data.prepared = [some_preprepare(1, 11, "digest11"),
                     some_preprepare(1, 12, "digest12")]
    data.preprepared = [some_preprepare(1, 11, "digest11"),
                        some_preprepare(1, 12, "digest12"),
                        some_preprepare(1, 13, "digest13")]
    view_change_service.start_view_change()

    assert data.prepared == []
    assert data.preprepared == []
    assert data.view_no == 2

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(1, 11, "digest11"), (1, 12, "digest12")]
    assert msg.preprepared == [(1, 11, "digest11"), (1, 12, "digest12"), (1, 13, "digest13")]
    assert msg.stableCheckpoint == 10
    assert msg.checkpoints == [cp1, cp2]


def test_view_change_empty_prepares(view_change_service, data):
    data.prepared = []
    data.preprepared = []

    view_change_service.start_view_change()

    assert data.prepared == []
    assert data.preprepared == []

    msg = get_view_change(view_change_service)
    assert msg.prepared == []
    assert msg.preprepared == []


def test_view_change_replaces_prepare(view_change_service, data):
    data.view_no = 0
    data.prepared = [some_preprepare(0, 1, "digest1"),
                     some_preprepare(0, 2, "digest2")]

    # view no 0->1
    view_change_service.start_view_change()

    assert data.view_no == 1
    assert data.prepared == []

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.prepared == [(0, 1, "digest1"), (0, 2, "digest2")]

    # view no 1->2
    # replace by different viewNo and digest
    data.prepared = [some_preprepare(1, 1, "digest11"),
                     some_preprepare(1, 2, "digest22")]
    view_change_service.start_view_change()

    assert data.view_no == 2
    assert data.prepared == []

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.prepared == [(1, 1, "digest11"), (1, 2, "digest22")]

    # view no 2->3
    # replace by different viewNo only
    data.prepared = [some_preprepare(2, 2, "digest22"),
                     some_preprepare(2, 3, "digest3")]
    view_change_service.start_view_change()

    assert data.view_no == 3
    assert data.prepared == []

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 3
    assert msg.prepared == [(1, 1, "digest11"), (2, 2, "digest22"), (2, 3, "digest3")]


def test_view_change_keeps_preprepare(view_change_service, data):
    data.view_no = 0
    data.preprepared = [some_preprepare(0, 1, "digest1"),
                        some_preprepare(0, 2, "digest2")]

    # view no 0->1
    view_change_service.start_view_change()

    assert data.view_no == 1
    assert data.preprepared == []

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 1
    assert msg.preprepared == [(0, 1, "digest1"), (0, 2, "digest2")]

    # view no 1->2
    # do not replace since different viewNo and digest
    data.preprepared = [some_preprepare(1, 1, "digest11"),
                        some_preprepare(1, 2, "digest22")]
    view_change_service.start_view_change()

    assert data.view_no == 2
    assert data.preprepared == []

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 2
    assert msg.preprepared == [(0, 1, "digest1"), (0, 2, "digest2"),
                               (1, 1, "digest11"), (1, 2, "digest22")]

    # view no 2->3
    #  replace by different viewNo only
    data.preprepared = [some_preprepare(2, 2, "digest22"),
                        some_preprepare(2, 3, "digest3")]
    view_change_service.start_view_change()

    assert data.view_no == 3
    assert data.preprepared == []

    msg = get_view_change(view_change_service)
    assert msg.viewNo == 3
    assert msg.preprepared == [(0, 1, "digest1"), (0, 2, "digest2"),
                               (1, 1, "digest11"),
                               (2, 2, "digest22"), (2, 3, "digest3")]


def test_different_view_change_messages_have_different_digests(view_change_message, random):
    vc = view_change_message(random.integer(0, 10000))
    other_vc = view_change_message(random.integer(0, 10000))
    assert view_change_digest(vc) != view_change_digest(other_vc)


def test_view_change_digest_is_256_bit_hexdigest(view_change_message, random):
    vc = view_change_message(random.integer(0, 10000))
    digest = view_change_digest(vc)
    assert isinstance(digest, str)
    assert len(digest) == 64
    assert all(v in string.hexdigits for v in digest)

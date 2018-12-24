import pytest

from common.exceptions import LogicError
from plenum.server.replica_stasher import ReplicaStasher
from plenum.server.replica_validator_enums import STASH_CATCH_UP, STASH_WATERMARKS, STASH_VIEW
from plenum.test.helper import create_pre_prepare_no_bls, create_prepare, create_commit_no_bls_sig, generate_state_root


@pytest.fixture(scope='function')
def replica_stasher(replica):
    return ReplicaStasher(replica=replica)


@pytest.fixture(scope='function')
def three_pc_msgs(replica):
    return create_three_pc_msgs(replica, 1)


def create_three_pc_msgs(replica, pp_seq_no):
    pre_prepare = create_pre_prepare_no_bls(generate_state_root(),
                                            view_no=replica.viewNo,
                                            pp_seq_no=pp_seq_no,
                                            inst_id=replica.instId)
    prepare = create_prepare(req_key=(replica.viewNo, pp_seq_no),
                             state_root=generate_state_root(),
                             inst_id=replica.instId)
    commit = create_commit_no_bls_sig(req_key=(replica.viewNo, pp_seq_no),
                                      inst_id=replica.instId)
    return [pre_prepare, prepare, commit]


@pytest.fixture(scope='function')
def stash_all(replica_stasher):
    msgs_catchup = create_three_pc_msgs(replica_stasher.replica, 1)
    msgs_watermarks = create_three_pc_msgs(replica_stasher.replica, 2)
    msgs_view = create_three_pc_msgs(replica_stasher.replica, 3)

    for msg in msgs_catchup:
        replica_stasher.stash(msg, STASH_CATCH_UP)
    for msg in msgs_watermarks:
        replica_stasher.stash(msg, STASH_WATERMARKS)
    for msg in msgs_view:
        replica_stasher.stash(msg, STASH_VIEW)

    return msgs_catchup, msgs_watermarks, msgs_view


def test_stash_catch_up_msgs(replica_stasher, three_pc_msgs):
    assert replica_stasher.num_stashed_catchup == 0
    for msg in three_pc_msgs:
        replica_stasher.stash(msg, STASH_CATCH_UP)
    assert replica_stasher.num_stashed_catchup == 3


def test_stash_future_view_msgs(replica_stasher, three_pc_msgs):
    assert replica_stasher.num_stashed_future_view == 0
    for msg in three_pc_msgs:
        replica_stasher.stash(msg, STASH_VIEW)
    assert replica_stasher.num_stashed_future_view == 3


def test_stash_watermarks_msgs(replica_stasher, three_pc_msgs):
    assert replica_stasher.num_stashed_watermarks == 0
    for msg in three_pc_msgs:
        replica_stasher.stash(msg, STASH_WATERMARKS)
    assert replica_stasher.num_stashed_watermarks == 3


def test_stash_unknown(replica_stasher, three_pc_msgs):
    for msg in three_pc_msgs:
        with pytest.raises(LogicError):
            replica_stasher.stash(msg, "UnknownStashType")


def test_unstash_catchup(replica_stasher, stash_all):
    msgs_catchup, msgs_watermarks, msgs_view = stash_all

    replica_stasher.unstash_catchup()

    assert replica_stasher.num_stashed_catchup == 0
    assert replica_stasher.num_stashed_watermarks == 3
    assert replica_stasher.num_stashed_future_view == 3

    assert list(replica_stasher.replica.inBox) == msgs_catchup


def test_unstash_future_view(replica_stasher, stash_all):
    msgs_catchup, msgs_watermarks, msgs_view = stash_all

    replica_stasher.unstash_future_view()

    assert replica_stasher.num_stashed_catchup == 3
    assert replica_stasher.num_stashed_watermarks == 3
    assert replica_stasher.num_stashed_future_view == 0

    assert list(replica_stasher.replica.inBox) == msgs_view


def test_unstash_watermarks(replica_stasher, stash_all):
    msgs_catchup, msgs_watermarks, msgs_view = stash_all

    replica_stasher.unstash_watermarks()

    assert replica_stasher.num_stashed_catchup == 3
    assert replica_stasher.num_stashed_watermarks == 0
    assert replica_stasher.num_stashed_future_view == 3

    assert list(replica_stasher.replica.inBox) == msgs_watermarks


def test_unstash_all(replica_stasher, stash_all):
    msgs_catchup, msgs_watermarks, msgs_view = stash_all

    replica_stasher.unstash_watermarks()
    replica_stasher.unstash_future_view()
    replica_stasher.unstash_catchup()

    assert replica_stasher.num_stashed_catchup == 0
    assert replica_stasher.num_stashed_watermarks == 0
    assert replica_stasher.num_stashed_future_view == 0

    assert list(replica_stasher.replica.inBox) == msgs_watermarks + msgs_view + msgs_catchup

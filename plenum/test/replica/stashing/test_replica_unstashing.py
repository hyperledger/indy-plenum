import pytest

from plenum.server.replica_validator_enums import STASH_CATCH_UP, STASH_WATERMARKS, STASH_VIEW
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root


@pytest.fixture(scope='function')
def msg(replica):
    pp = create_pre_prepare_no_bls(generate_state_root(),
                                   view_no=replica.viewNo,
                                   pp_seq_no=replica.last_ordered_3pc[1] + 1,
                                   inst_id=replica.instId)
    return replica.primaryName, pp


def test_unstash_catchup(replica, msg):
    _, pre_prepare = msg
    replica.stasher.stash(msg, STASH_CATCH_UP)
    assert replica.stasher.num_stashed_catchup > 0
    replica.on_catch_up_finished()
    assert replica.stasher.num_stashed_catchup == 0
    assert replica.inBox.popleft() == msg


def test_unstash_future_view(replica, msg):
    replica.stasher.stash(msg, STASH_VIEW)
    assert replica.stasher.num_stashed_future_view > 0
    replica.on_view_change_done()
    assert replica.stasher.num_stashed_future_view == 0
    assert replica.inBox.popleft() == msg


def test_unstash_watermarks(replica, msg, looper):
    _, pre_prepare = msg
    replica.stasher.stash(msg, STASH_WATERMARKS)
    assert replica.stasher.num_stashed_watermarks > 0
    replica.h = pre_prepare.ppSeqNo
    assert replica.stasher.num_stashed_watermarks == 0
    assert replica.inBox.popleft() == msg

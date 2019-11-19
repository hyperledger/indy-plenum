import pytest

from plenum.server.replica_validator_enums import STASH_CATCH_UP, STASH_WATERMARKS, STASH_VIEW_3PC
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root
from plenum.test.replica.helper import emulate_catchup


@pytest.fixture(scope='function')
def msg(replica):
    pp = create_pre_prepare_no_bls(generate_state_root(),
                                   view_no=replica.viewNo,
                                   pp_seq_no=replica.last_ordered_3pc[1] + 1,
                                   inst_id=replica.instId)
    return pp, replica.primaryName


def test_unstash_catchup(replica, msg):
    pre_prepare, _ = msg
    replica.stasher._stash(STASH_CATCH_UP, "reason", *msg)
    assert replica.stasher.stash_size(STASH_CATCH_UP) > 0
    emulate_catchup(replica)
    assert replica.stasher.stash_size(STASH_CATCH_UP) == 0


def test_unstash_future_view(replica, msg):
    pre_prepare, _ = msg
    replica.stasher._stash(STASH_VIEW_3PC, "reason", *msg)
    assert replica.stasher.stash_size(STASH_VIEW_3PC) > 0
    replica.on_view_propagated_after_catchup()
    assert replica.stasher.stash_size(STASH_VIEW_3PC) == 0


def test_unstash_watermarks(replica, msg, looper):
    pre_prepare, _ = msg
    replica.last_ordered_3pc = (replica.viewNo, pre_prepare.ppSeqNo)
    replica.stasher._stash(STASH_WATERMARKS, "reason", *msg)
    assert replica.stasher.stash_size(STASH_WATERMARKS) > 0
    replica._checkpointer.set_watermarks(low_watermark=pre_prepare.ppSeqNo)
    assert replica.stasher.stash_size(STASH_WATERMARKS) == 0

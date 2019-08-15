import pytest

from plenum.common.messages.internal_messages import NewViewCheckpointsApplied
from plenum.common.messages.node_messages import NewView
from plenum.common.startable import Mode
from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.server.replica_validator_enums import PROCESS, DISCARD, STASH_VIEW, STASH_CATCH_UP, STASH_WATERMARKS, \
    STASH_WAITING_NEW_VIEW, OLD_VIEW, OUTSIDE_WATERMARKS, ALREADY_ORDERED, CATCHING_UP, FUTURE_VIEW, \
    WAITING_FOR_NEW_VIEW
from plenum.test.bls.helper import generate_state_root
from plenum.test.helper import create_pre_prepare_no_bls, create_prepare, create_commit_no_bls_sig


@pytest.fixture(scope='function', params=[0, 2])
def view_no(tconf, request):
    return request.param


@pytest.fixture(scope='function', params=[Mode.starting,
                                          Mode.discovering,
                                          Mode.discovered,
                                          Mode.syncing,
                                          Mode.synced,
                                          Mode.participating])
def mode(tconf, request):
    return request.param


@pytest.fixture(scope='function', params=[True, False])
def waiting_for_new_view(tconf, request):
    return request.param


@pytest.fixture(scope='function')
def validator(consensus_data, view_no):
    cd = consensus_data("For3PCValidator")
    cd.pp_seq_no = 1
    cd.view_no = view_no
    cd.node_mode = Mode.participating
    return OrderingServiceMsgValidator(data=cd)


def pre_prepare(view_no, pp_seq_no):
    return create_pre_prepare_no_bls(generate_state_root(),
                                     view_no=view_no,
                                     pp_seq_no=pp_seq_no,
                                     inst_id=0)


def prepare(view_no, pp_seq_no):
    return create_prepare(req_key=(view_no, pp_seq_no),
                          state_root=generate_state_root(),
                          inst_id=0)


def commit(view_no, pp_seq_no):
    return create_commit_no_bls_sig(req_key=(view_no, pp_seq_no),
                                    inst_id=0)


def new_view(view_no):
    return NewViewCheckpointsApplied(view_no, [], [], [])


def test_process_correct_pre_prepare(validator, view_no):
    assert validator.validate_pre_prepare(
        pre_prepare(view_no=view_no, pp_seq_no=1)) == (PROCESS, None)


def test_process_correct_prepare(validator, view_no):
    assert validator.validate_prepare(
        prepare(view_no=view_no, pp_seq_no=1)) == (PROCESS, None)


def test_process_correct_commit(validator, view_no):
    assert validator.validate_commit(
        commit(view_no=view_no, pp_seq_no=1)) == (PROCESS, None)


def test_process_correct_new_view(validator, view_no):
    assert validator.validate_new_view(
        new_view(view_no=view_no)) == (PROCESS, None)


def test_discard_old_view(validator, view_no, mode, waiting_for_new_view):
    validator._data.node_mode = mode
    validator._data.waiting_for_new_view = waiting_for_new_view

    validator._data.view_no = view_no + 2
    assert validator.validate_pre_prepare(pre_prepare(view_no, 1)) == (DISCARD, OLD_VIEW)
    assert validator.validate_prepare(prepare(view_no, 1)) == (DISCARD, OLD_VIEW)
    assert validator.validate_commit(commit(view_no, 1)) == (DISCARD, OLD_VIEW)
    assert validator.validate_new_view(new_view(view_no)) == (DISCARD, OLD_VIEW)

    validator._data.view_no = view_no + 1
    assert validator.validate_pre_prepare(pre_prepare(view_no, 1)) == (DISCARD, OLD_VIEW)
    assert validator.validate_prepare(prepare(view_no, 1)) == (DISCARD, OLD_VIEW)
    assert validator.validate_commit(commit(view_no, 1)) == (DISCARD, OLD_VIEW)
    assert validator.validate_new_view(new_view(view_no)) == (DISCARD, OLD_VIEW)


@pytest.mark.parametrize('pp_seq_no, result', [
    (1, (DISCARD, ALREADY_ORDERED)),
    (50, (DISCARD, ALREADY_ORDERED)),
    (99, (DISCARD, ALREADY_ORDERED)),
    (100, (DISCARD, ALREADY_ORDERED)),
    (101, (PROCESS, None)),
    (300, (PROCESS, None)),
    (399, (PROCESS, None)),
])
def test_discard_below_watermark_3pc(validator, view_no, pp_seq_no, result):
    validator._data.low_watermark = 100
    validator._data.high_watermark = 400
    assert validator.validate_pre_prepare(pre_prepare(view_no, pp_seq_no)) == result
    assert validator.validate_prepare(prepare(view_no, pp_seq_no)) == result
    assert validator.validate_commit(commit(view_no, pp_seq_no)) == result


def test_discard_below_watermark_3pc_no_stash(validator, view_no, mode, waiting_for_new_view):
    validator._data.node_mode = mode
    validator._data.waiting_for_new_view = waiting_for_new_view
    validator._data.low_watermark = 100
    validator._data.high_watermark = 400
    assert validator.validate_pre_prepare(pre_prepare(view_no, 99)) == (DISCARD, ALREADY_ORDERED)
    assert validator.validate_prepare(prepare(view_no, 99)) == (DISCARD, ALREADY_ORDERED)
    assert validator.validate_commit(commit(view_no, 99)) == (DISCARD, ALREADY_ORDERED)


@pytest.mark.parametrize('pp_seq_no, result', [
    (1, (DISCARD, ALREADY_ORDERED)),
    (9, (DISCARD, ALREADY_ORDERED)),
    (10, (DISCARD, ALREADY_ORDERED)),
    (11, (PROCESS, None)),
    (12, (PROCESS, None)),
    (100, (PROCESS, None))
])
def test_discard_ordered_pre_prepare(validator, view_no, pp_seq_no, result):
    validator._data.last_ordered_3pc = (view_no, 10)
    assert validator.validate_pre_prepare(pre_prepare(view_no, pp_seq_no)) == result


@pytest.mark.parametrize('pp_seq_no', [1, 5, 9, 10])
def test_discard_ordered_pre_prepare_no_stash(validator, view_no, pp_seq_no, mode, waiting_for_new_view):
    validator._data.last_ordered_3pc = (view_no, 10)
    assert validator.validate_pre_prepare(pre_prepare(view_no, pp_seq_no)) == (DISCARD, ALREADY_ORDERED)


@pytest.mark.parametrize('mode, result', [
    (Mode.starting, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.discovering, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.discovered, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.syncing, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.synced, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.participating, (PROCESS, None)),
])
def test_stash_while_catchup(validator, view_no, mode, result):
    validator._data.node_mode = mode
    assert validator.validate_pre_prepare(pre_prepare(view_no, 1)) == result
    assert validator.validate_prepare(prepare(view_no, 1)) == result
    assert validator.validate_commit(commit(view_no, 1)) == result
    assert validator.validate_new_view(new_view(view_no)) == result


def test_stash_future_view(validator, view_no):
    assert validator.validate_pre_prepare(pre_prepare(view_no + 1, 1)) == (STASH_VIEW, FUTURE_VIEW)
    assert validator.validate_prepare(prepare(view_no + 1, 1)) == (STASH_VIEW, FUTURE_VIEW)
    assert validator.validate_commit(commit(view_no + 1, 1)) == (STASH_VIEW, FUTURE_VIEW)
    assert validator.validate_new_view(new_view(view_no + 1)) == (STASH_VIEW, FUTURE_VIEW)

    assert validator.validate_pre_prepare(pre_prepare(view_no + 2, 1)) == (STASH_VIEW, FUTURE_VIEW)
    assert validator.validate_prepare(prepare(view_no + 2, 1)) == (STASH_VIEW, FUTURE_VIEW)
    assert validator.validate_commit(commit(view_no + 2, 1)) == (STASH_VIEW, FUTURE_VIEW)
    assert validator.validate_new_view(new_view(view_no + 2)) == (STASH_VIEW, FUTURE_VIEW)


def test_stash_waiting_for_new_view_3pc(validator, view_no):
    validator._data.waiting_for_new_view = True
    assert validator.validate_pre_prepare(pre_prepare(view_no, 1)) == (STASH_WAITING_NEW_VIEW, WAITING_FOR_NEW_VIEW)
    assert validator.validate_prepare(prepare(view_no, 1)) == (STASH_WAITING_NEW_VIEW, WAITING_FOR_NEW_VIEW)
    assert validator.validate_commit(commit(view_no, 1)) == (STASH_WAITING_NEW_VIEW, WAITING_FOR_NEW_VIEW)


@pytest.mark.parametrize('pp_seq_no, result', [
    (101, (PROCESS, None)),
    (300, (PROCESS, None)),
    (399, (PROCESS, None)),
    (400, (PROCESS, None)),
    (401, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (402, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (100000, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
])
def test_stash_above_watermark_3pc(validator, view_no, pp_seq_no, result):
    validator._data.low_watermark = 100
    validator._data.high_watermark = 400
    assert validator.validate_pre_prepare(pre_prepare(view_no, pp_seq_no)) == result
    assert validator.validate_prepare(prepare(view_no, pp_seq_no)) == result
    assert validator.validate_commit(commit(view_no, pp_seq_no)) == result


def test_process_waiting_for_new_view_new_view(validator, view_no):
    validator._data.waiting_for_new_view = True
    assert validator.validate_new_view(new_view(view_no)) == (PROCESS, None)


@pytest.mark.parametrize('pp_seq_no', [1, 9, 10, 11, 12, 100])
def test_process_ordered_prepare_commit(validator, view_no, pp_seq_no):
    validator._data.last_ordered_3pc = (view_no, 10)
    assert validator.validate_prepare(prepare(view_no, pp_seq_no)) == (PROCESS, None)
    assert validator.validate_commit(commit(view_no, pp_seq_no)) == (PROCESS, None)

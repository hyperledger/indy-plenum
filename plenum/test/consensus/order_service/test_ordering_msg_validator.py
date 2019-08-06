import pytest

from plenum.common.startable import Mode
from plenum.server.consensus.consensus_shared_data import BatchID
from plenum.server.consensus.msg_validator import OrderingServiceMsgValidator
from plenum.server.consensus.ordering_service import ThreePCMsgValidator
from plenum.server.replica_validator_enums import PROCESS, DISCARD, INCORRECT_PP_SEQ_NO, ALREADY_ORDERED, FUTURE_VIEW, \
    STASH_VIEW, OLD_VIEW, STASH_CATCH_UP, CATCHING_UP, OUTSIDE_WATERMARKS, STASH_WATERMARKS, GREATER_PREP_CERT
from plenum.test.bls.helper import generate_state_root
from plenum.test.helper import create_pre_prepare_no_bls, create_prepare, create_commit_no_bls_sig


@pytest.fixture(scope='function', params=[0, 1])
def inst_id(request):
    return request.param


@pytest.fixture(scope='function', params=[0, 2])
def view_no(tconf, request):
    return request.param


@pytest.fixture(scope='function')
def validator(consensus_data, view_no):
    cd = consensus_data("For3PCValidator")
    cd.pp_seq_no = 1
    cd.view_no = view_no
    cd.node_mode = Mode.participating
    return OrderingServiceMsgValidator(data=cd)


@pytest.fixture(scope='function', params=[1, 2, 3])
def pp_seq_no(request):
    return request.param


def create_3pc_msgs(view_no, pp_seq_no, inst_id):
    pre_prepare = create_pre_prepare_no_bls(generate_state_root(),
                                            view_no=view_no,
                                            pp_seq_no=pp_seq_no,
                                            inst_id=inst_id)
    prepare = create_prepare(req_key=(view_no, pp_seq_no),
                             state_root=generate_state_root(),
                             inst_id=inst_id)
    commit = create_commit_no_bls_sig(req_key=(view_no, pp_seq_no),
                                      inst_id=inst_id)
    return [pre_prepare, prepare, commit]


def test_check_all_correct(validator, inst_id):
    validator._data.node_mode = Mode.participating
    for msg in create_3pc_msgs(view_no=validator._data.view_no,
                               pp_seq_no=1,
                               inst_id=inst_id):
        assert validator.validate(msg) == (PROCESS, None)


@pytest.mark.parametrize('mode, result', [
    (Mode.starting, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.discovering, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.discovered, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.syncing, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.synced, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.participating, (PROCESS, None)),
])
def test_check_while_catchup(validator, mode, result, inst_id):
    validator._data.node_mode = mode
    for msg in create_3pc_msgs(view_no=validator._data.view_no,
                               pp_seq_no=1,
                               inst_id=inst_id):
        assert validator.validate(msg) == result


def test_check_current_view(validator, inst_id, view_no):
    validator._data.view_no = view_no
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=1,
                               inst_id=inst_id):
        assert validator.validate(msg) == (PROCESS, None)


def test_check_old_view(validator, inst_id, view_no):
    validator._data.view_no = view_no + 2
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=1,
                               inst_id=inst_id):
        assert validator.validate(msg) == (DISCARD, OLD_VIEW)


def test_check_future_view(validator, inst_id):
    for msg in create_3pc_msgs(view_no=validator._data.view_no + 1,
                               pp_seq_no=1,
                               inst_id=inst_id):
        assert validator.validate(msg) == (STASH_VIEW, FUTURE_VIEW)


@pytest.mark.parametrize('mode', [
    Mode.starting,
    Mode.discovering,
    Mode.discovered,
    Mode.syncing,
    Mode.synced,
    Mode.participating
])
@pytest.mark.parametrize('waiting_for_new_view', [True, False])
def test_check_previous_view(validator, inst_id, view_no, mode, waiting_for_new_view):
    validator._data.waiting_for_new_view = waiting_for_new_view
    validator._data.node_mode = mode
    validator._data.view_no = view_no + 1
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=1,
                               inst_id=inst_id):
        assert validator.validate(msg) == (DISCARD, OLD_VIEW)


# assume last ordered is 10
@pytest.mark.parametrize('pp_seq_no', [0, 1, 9, 10, 11, 12, 100])
def test_check_ordered(validator, pp_seq_no, view_no, inst_id):
    validator._data.last_ordered_3pc = (view_no, 10)
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=inst_id):
        assert validator.validate(msg) == (PROCESS, None)


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, (DISCARD, INCORRECT_PP_SEQ_NO)),
    (1, (PROCESS, None)),
    (100, (PROCESS, None)),
    (299, (PROCESS, None)),
    (300, (PROCESS, None)),
    # assume [0, 300]
    (301, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (302, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (100000, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
])
def test_check_watermarks_default(validator, pp_seq_no, result, view_no, inst_id):
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=inst_id):
        assert validator.validate(msg) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    # assume [100, 400]
    (0, (DISCARD, INCORRECT_PP_SEQ_NO)),
    (1, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (99, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (100, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (101, (PROCESS, None)),
    (400, (PROCESS, None)),
    (401, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (402, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
    (100000, (STASH_WATERMARKS, OUTSIDE_WATERMARKS)),
])
def test_check_watermarks_changed(validator, pp_seq_no, result, view_no, inst_id):
    validator._data.low_watermark = 100
    validator._data.high_watermark = 400
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=inst_id):
        assert validator.validate(msg) == result


def test_check_zero_pp_seq_no(validator, view_no, inst_id):
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=0,
                               inst_id=inst_id):
        assert validator.validate(msg) == (DISCARD, INCORRECT_PP_SEQ_NO)


@pytest.mark.parametrize('pp_seq_no', [0, 1, 9, 10, 11, 12, 100])
def test_check_ordered_not_participating(validator, pp_seq_no, inst_id):
    validator._data.last_ordered_3pc = (validator._data.view_no, 10)
    validator._data.node_mode = Mode.syncing
    for msg in create_3pc_msgs(view_no=validator._data.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=inst_id):
        assert validator.validate(msg) == (STASH_CATCH_UP, CATCHING_UP)

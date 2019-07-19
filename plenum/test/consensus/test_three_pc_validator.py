import pytest

from plenum.common.startable import Mode
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
    return ThreePCMsgValidator(data=cd)


@pytest.fixture(scope='function', params=[1, 2, 3, 4])
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


def test_check_all_correct(validator, view_no, inst_id):
    validator._data.is_participating = True
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=1 + 1,
                               inst_id=inst_id):
        assert validator.validate(msg) == (PROCESS, None)


def test_discard_0_pp_seq_no(validator, view_no, inst_id):
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=0,
                               inst_id=inst_id):
        assert validator.validate(msg) == (DISCARD, INCORRECT_PP_SEQ_NO)


def test_discard_already_ordered(validator, view_no, pp_seq_no, inst_id):
    validator._data.view_no = view_no
    validator._data.pp_seq_no = pp_seq_no
    validator._data.last_ordered_3pc = (view_no, pp_seq_no)
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=inst_id):
        assert validator.validate(msg) == (DISCARD, ALREADY_ORDERED)


def test_future_view_no(validator, view_no, pp_seq_no, inst_id):
    validator._data.view_no = view_no
    for msg in create_3pc_msgs(view_no=view_no + 1,
                               pp_seq_no=pp_seq_no,
                               inst_id=inst_id):
        assert validator.validate(msg) == (STASH_VIEW, FUTURE_VIEW)


def test_old_view_no(validator, view_no, pp_seq_no, inst_id):
    """The situation is (for example):
        last_ordered = (0,0)
        view_no = 2
        Batches 3pc = (0,1)
    """

    validator._data.view_no = view_no + 2
    validator._data.last_ordered_3pc = (view_no, pp_seq_no)
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no + 1,
                               inst_id=inst_id):
        assert validator.validate(msg) == (DISCARD, OLD_VIEW)


def test_future_view_no_vc_in_progress(validator, view_no, pp_seq_no, inst_id):
    validator._data.view_no = view_no
    validator._data.legacy_vc_in_progress = True
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=inst_id):
        assert validator.validate(msg) == (STASH_VIEW, FUTURE_VIEW)


def test_is_not_participating(validator, view_no, inst_id, pp_seq_no):
    validator._data.is_participating = False
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no + 1,
                               inst_id=inst_id):
        assert validator.validate(msg) == (STASH_CATCH_UP, CATCHING_UP)


def test_outside_watermark(validator, view_no, inst_id, pp_seq_no):
    H = 10
    validator._data.is_participating = True
    validator._data.low_watermark = 0
    validator._data.high_watermark = H
    for msg in create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no + H,
                               inst_id=inst_id):
        assert validator.validate(msg) == (STASH_WATERMARKS, OUTSIDE_WATERMARKS)


def test_not_commit_with_old_view(validator, view_no, inst_id, pp_seq_no):
    validator._data.view_no = view_no + 1
    validator._data.pp_seq_no = pp_seq_no
    pp, p, _ = create_3pc_msgs(view_no=view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=inst_id)
    assert validator.validate(pp) == (DISCARD, OLD_VIEW)
    assert validator.validate(p) == (DISCARD, OLD_VIEW)


def test_commit_old_view_vc_in_progress(validator, view_no, inst_id, pp_seq_no):
    validator._data.view_no = view_no + 1
    validator._data.pp_seq_no = pp_seq_no
    validator._data.legacy_vc_in_progress = False
    _, _, commit = create_3pc_msgs(view_no=view_no,
                                   pp_seq_no=pp_seq_no,
                                   inst_id=inst_id)
    assert validator.validate(commit) == (DISCARD, OLD_VIEW)


def test_commit_legacy_last_prepared_sertificate_is_none(validator, view_no, inst_id, pp_seq_no):
    validator._data.view_no = view_no + 1
    validator._data.pp_seq_no = pp_seq_no
    validator._data.legacy_vc_in_progress = True
    validator._data.legacy_last_prepared_before_view_change = None
    _, _, commit = create_3pc_msgs(view_no=view_no,
                                   pp_seq_no=pp_seq_no,
                                   inst_id=inst_id)
    assert validator.validate(commit) == (DISCARD, OLD_VIEW)


def test_commit_greater_then_legacy_last_prepared_sertificate(validator, view_no, inst_id, pp_seq_no):
    validator._data.view_no = view_no + 1
    validator._data.pp_seq_no = pp_seq_no
    validator._data.legacy_vc_in_progress = True
    validator._data.legacy_last_prepared_before_view_change = (view_no, pp_seq_no - 1)
    _, _, commit = create_3pc_msgs(view_no=view_no,
                                   pp_seq_no=pp_seq_no,
                                   inst_id=inst_id)
    assert validator.validate(commit) == (DISCARD, GREATER_PREP_CERT)


def test_process_if_synced_and_vc_in_progress(validator, view_no, inst_id, pp_seq_no):
    validator._data.view_no = view_no + 1
    validator._data.node_mode = Mode.synced
    validator._data.legacy_vc_in_progress = True
    validator._data.legacy_last_prepared_before_view_change = (view_no, pp_seq_no + 1)
    _, _, commit = create_3pc_msgs(view_no=view_no,
                                   pp_seq_no=pp_seq_no + 1,
                                   inst_id=inst_id)

    assert validator.validate(commit) == (PROCESS, None)

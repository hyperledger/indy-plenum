import functools

import pytest

from plenum.common.startable import Mode
from plenum.server.node import Node
from plenum.server.replica_validator import ReplicaValidator
from plenum.server.replica_validator_enums import DISCARD, INCORRECT_INSTANCE, PROCESS, ALREADY_ORDERED, FUTURE_VIEW, \
    GREATER_PREP_CERT, OLD_VIEW, CATCHING_UP, OUTSIDE_WATERMARKS, INCORRECT_PP_SEQ_NO, STASH_VIEW, STASH_WATERMARKS, \
    STASH_CATCH_UP
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root, create_commit_no_bls_sig, create_prepare


@pytest.fixture(scope='function', params=[0, 1])
def inst_id(request):
    return request.param


@pytest.fixture(scope='function', params=[2])
def viewNo(tconf, request):
    return request.param


@pytest.fixture(scope='function')
def validator(replica, inst_id):
    return ReplicaValidator(replica=replica)


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


def test_check_all_correct(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == (PROCESS, None)


def test_check_inst_id_incorrect(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=1,
                               inst_id=validator.inst_id + 1):
        assert validator.validate_3pc_msg(msg) == (DISCARD, INCORRECT_INSTANCE)


@pytest.mark.parametrize('mode, result', [
    (Mode.starting, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.discovering, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.discovered, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.syncing, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.synced, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.participating, (PROCESS, None)),
])
def test_check_participating(validator, mode, result):
    validator.replica.node.mode = mode
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == result


def test_check_current_view(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == (PROCESS, None)


def test_check_old_view(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no - 2,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == (DISCARD, OLD_VIEW)


def test_check_future_view(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no + 1,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == (STASH_VIEW, FUTURE_VIEW)


def test_check_previous_view_no_view_change(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no - 1,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == (DISCARD, OLD_VIEW)


def test_check_previous_view_view_change_no_prep_cert(validator):
    validator.replica.node.view_change_in_progress = True
    for msg in create_3pc_msgs(view_no=validator.view_no - 1,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == (DISCARD, OLD_VIEW)


@pytest.mark.parametrize('mode, result', [
    (Mode.starting, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.discovering, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.discovered, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.syncing, (STASH_CATCH_UP, CATCHING_UP)),
    (Mode.synced, (PROCESS, None)),
    (Mode.participating, (PROCESS, None))
])
def test_check_catchup_modes_in_view_change_for_prep_cert(validator, result, mode):
    pp_seq_no = 10
    validator.replica.node.view_change_in_progress = True
    validator.replica.node.mode = mode
    validator.replica.last_prepared_before_view_change = (validator.view_no - 1,
                                                          pp_seq_no)
    for msg in create_3pc_msgs(view_no=validator.view_no - 1,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, (DISCARD, INCORRECT_PP_SEQ_NO)),
    (1, (PROCESS, None)),
    (9, (PROCESS, None)),
    (10, (PROCESS, None)),
    # assume prep cert is 10
    (11, (DISCARD, GREATER_PREP_CERT)),
    (12, (DISCARD, GREATER_PREP_CERT)),
    (100, (DISCARD, GREATER_PREP_CERT)),
])
def test_check_previous_view_view_change_prep_cert(validator, pp_seq_no, result):
    validator.replica.node.view_change_in_progress = True
    validator.replica.last_prepared_before_view_change = (validator.view_no - 1, 10)
    for msg in create_3pc_msgs(view_no=validator.view_no - 1,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, (DISCARD, INCORRECT_PP_SEQ_NO)),
    (1, (STASH_VIEW, FUTURE_VIEW)),
    (9, (STASH_VIEW, FUTURE_VIEW)),
    (10, (STASH_VIEW, FUTURE_VIEW)),
    (11, (STASH_VIEW, FUTURE_VIEW)),
    (12, (STASH_VIEW, FUTURE_VIEW)),
    (100, (STASH_VIEW, FUTURE_VIEW)),
])
def test_check_current_view_view_change_prep_cert(validator, pp_seq_no, result):
    validator.replica.node.view_change_in_progress = True
    validator.replica.last_prepared_before_view_change = (validator.view_no - 1, 10)
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, (DISCARD, INCORRECT_PP_SEQ_NO)),
    (1, (DISCARD, ALREADY_ORDERED)),
    (9, (DISCARD, ALREADY_ORDERED)),
    (10, (DISCARD, ALREADY_ORDERED)),
    # assume last ordered is 10
    (11, (PROCESS, None)),
    (12, (PROCESS, None)),
    (100, (PROCESS, None)),
])
def test_check_ordered(validator, pp_seq_no, result):
    validator.replica.last_ordered_3pc = (validator.view_no, 10)
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == result


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
def test_check_watermarks_default(validator, pp_seq_no, result):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == result


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
def test_check_watermarks_changed(validator, pp_seq_no, result):
    validator.replica.h = 100
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == result


def test_check_zero_pp_seq_no(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=0,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == (DISCARD, INCORRECT_PP_SEQ_NO)


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, (DISCARD, INCORRECT_PP_SEQ_NO)),
    (1, (DISCARD, ALREADY_ORDERED)),
    (9, (DISCARD, ALREADY_ORDERED)),
    (10, (DISCARD, ALREADY_ORDERED)),
    # assume last ordered is 10
    (11, (STASH_CATCH_UP, CATCHING_UP)),
    (12, (STASH_CATCH_UP, CATCHING_UP)),
    (100, (STASH_CATCH_UP, CATCHING_UP)),
])
def test_check_ordered_not_participating(validator, pp_seq_no, result):
    validator.replica.last_ordered_3pc = (validator.view_no, 10)
    validator.replica.node.mode = Mode.syncing
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate_3pc_msg(msg) == result

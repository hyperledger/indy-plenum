import pytest

from plenum.common.startable import Mode
from plenum.server.replica_validator import ReplicaValidator, ReplicaValidationResult
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


def test_check_inst_id_correct(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == ReplicaValidationResult.PROCESS


def test_check_inst_id_incorrect(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=1,
                               inst_id=validator.inst_id + 1):
        assert validator.validate(msg) == ReplicaValidationResult.DISCARD


@pytest.mark.parametrize('mode, result', [
    (Mode.starting, ReplicaValidationResult.STASH),
    (Mode.discovering, ReplicaValidationResult.STASH),
    (Mode.discovered, ReplicaValidationResult.STASH),
    (Mode.syncing, ReplicaValidationResult.STASH),
    (Mode.synced, ReplicaValidationResult.STASH),
    (Mode.participating, ReplicaValidationResult.PROCESS),
])
def test_check_participating(validator, mode, result):
    validator.replica.node.mode = mode
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == result


def test_check_current_view(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == ReplicaValidationResult.PROCESS


def test_check_old_view(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no - 2,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == ReplicaValidationResult.DISCARD


def test_check_future_view(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no + 1,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == ReplicaValidationResult.STASH


def test_check_previous_view_no_view_change(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no - 1,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == ReplicaValidationResult.DISCARD


def test_check_previous_view_view_change_no_prep_cert(validator):
    validator.replica.node.view_change_in_progress = True
    for msg in create_3pc_msgs(view_no=validator.view_no - 1,
                               pp_seq_no=1,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == ReplicaValidationResult.DISCARD


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, ReplicaValidationResult.DISCARD),
    (1, ReplicaValidationResult.PROCESS),
    (9, ReplicaValidationResult.PROCESS),
    (10, ReplicaValidationResult.PROCESS),
    # assume prep cert is 10
    (11, ReplicaValidationResult.DISCARD),
    (12, ReplicaValidationResult.DISCARD),
    (100, ReplicaValidationResult.DISCARD),
])
def test_check_previous_view_view_change_prep_cert(validator, pp_seq_no, result):
    validator.replica.node.view_change_in_progress = True
    validator.replica.last_prepared_before_view_change = (validator.view_no - 1, 10)
    for msg in create_3pc_msgs(view_no=validator.view_no - 1,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, ReplicaValidationResult.DISCARD),
    (1, ReplicaValidationResult.STASH),
    (9, ReplicaValidationResult.STASH),
    (10, ReplicaValidationResult.STASH),
    (11, ReplicaValidationResult.STASH),
    (12, ReplicaValidationResult.STASH),
    (100, ReplicaValidationResult.STASH),
])
def test_check_current_view_view_change_prep_cert(validator, pp_seq_no, result):
    validator.replica.node.view_change_in_progress = True
    validator.replica.last_prepared_before_view_change = (validator.view_no - 1, 10)
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, ReplicaValidationResult.DISCARD),
    (1, ReplicaValidationResult.DISCARD),
    (9, ReplicaValidationResult.DISCARD),
    (10, ReplicaValidationResult.DISCARD),
    # assume last ordered is 10
    (11, ReplicaValidationResult.PROCESS),
    (12, ReplicaValidationResult.PROCESS),
    (100, ReplicaValidationResult.PROCESS),
])
def test_check_ordered(validator, pp_seq_no, result):
    validator.replica.last_ordered_3pc = (validator.view_no, 10)
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    (0, ReplicaValidationResult.DISCARD),
    (1, ReplicaValidationResult.PROCESS),
    (100, ReplicaValidationResult.PROCESS),
    (299, ReplicaValidationResult.PROCESS),
    (300, ReplicaValidationResult.PROCESS),
    # assume [0, 300]
    (301, ReplicaValidationResult.STASH),
    (302, ReplicaValidationResult.STASH),
    (100000, ReplicaValidationResult.STASH),
])
def test_check_watermarks_default(validator, pp_seq_no, result):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    # assume [100, 400]
    (0, ReplicaValidationResult.DISCARD),
    (1, ReplicaValidationResult.STASH),
    (99, ReplicaValidationResult.STASH),
    (100, ReplicaValidationResult.STASH),
    (101, ReplicaValidationResult.PROCESS),
    (400, ReplicaValidationResult.PROCESS),
    (401, ReplicaValidationResult.STASH),
    (402, ReplicaValidationResult.STASH),
    (100000, ReplicaValidationResult.STASH),
])
def test_check_watermarks_changed(validator, pp_seq_no, result):
    validator.replica.h = 100
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=pp_seq_no,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == result


def test_check_zero_pp_seq_no(validator):
    for msg in create_3pc_msgs(view_no=validator.view_no,
                               pp_seq_no=0,
                               inst_id=validator.inst_id):
        assert validator.validate(msg) == ReplicaValidationResult.DISCARD

import pytest

from plenum.common.messages.internal_messages import NewViewCheckpointsApplied
from plenum.common.messages.node_messages import OldViewPrePrepareRequest, OldViewPrePrepareReply
from plenum.common.startable import Mode, Status
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.server.replica_helper import generateName
from plenum.server.replica_validator_enums import PROCESS, DISCARD, STASH_CATCH_UP, STASH_WATERMARKS, \
    STASH_VIEW_3PC, OLD_VIEW, OUTSIDE_WATERMARKS, ALREADY_ORDERED, CATCHING_UP, FUTURE_VIEW, \
    WAITING_FOR_NEW_VIEW, NON_MASTER, INCORRECT_INSTANCE, STASH_WAITING_FIRST_BATCH_IN_VIEW, WAITING_FIRST_BATCH_IN_VIEW
from plenum.test.bls.helper import generate_state_root
from plenum.test.greek import genNodeNames
from plenum.test.helper import create_pre_prepare_no_bls, create_prepare, create_commit_no_bls_sig


@pytest.fixture(scope='function', params=[0, 2])
def view_no(request):
    return request.param


@pytest.fixture(scope='function', params=[Mode.starting,
                                          Mode.discovering,
                                          Mode.discovered,
                                          Mode.syncing,
                                          Mode.synced,
                                          Mode.participating])
def mode(request):
    return request.param


@pytest.fixture(scope='function', params=[True, False])
def waiting_for_new_view(request):
    return request.param


@pytest.fixture(scope='function')
def validator(view_no):
    validators = genNodeNames(4)
    inst_id = 0
    cd = ConsensusSharedData(generateName(validators[0], inst_id), validators, inst_id, True)
    cd.pp_seq_no = 1
    cd.view_no = view_no
    cd.node_mode = Mode.participating
    cd.node_status = Status.started
    cd.prev_view_prepare_cert = cd.last_ordered_3pc[1]
    return OrderingServiceMsgValidator(data=cd)


def pre_prepare(view_no, pp_seq_no, inst_id=0):
    return create_pre_prepare_no_bls(generate_state_root(),
                                     view_no=view_no,
                                     pp_seq_no=pp_seq_no,
                                     inst_id=inst_id)


def prepare(view_no, pp_seq_no, inst_id=0):
    return create_prepare(req_key=(view_no, pp_seq_no),
                          state_root=generate_state_root(),
                          inst_id=inst_id)


def commit(view_no, pp_seq_no, inst_id=0):
    return create_commit_no_bls_sig(req_key=(view_no, pp_seq_no),
                                    inst_id=inst_id)


def new_view(view_no):
    return NewViewCheckpointsApplied(view_no, [], [], [])


def old_view_pp_req():
    return OldViewPrePrepareRequest(0,
                                    [(1, 0, 1, "d1"), (1, 0, 2, "d1")])


def old_view_pp_rep():
    pp1 = create_pre_prepare_no_bls(generate_state_root(),
                                    view_no=0,
                                    pp_seq_no=1,
                                    inst_id=0)
    pp2 = create_pre_prepare_no_bls(generate_state_root(),
                                    view_no=0,
                                    pp_seq_no=1,
                                    inst_id=0)
    return OldViewPrePrepareReply(0,
                                  [pp1, pp2])


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
    assert validator.validate_new_view(new_view(view_no=view_no)) == (PROCESS, None)


def test_process_correct_old_view_pp_req(validator, view_no):
    assert validator.validate_old_view_prep_prepare_req(old_view_pp_req()) == (PROCESS, None)


def test_process_correct_old_view_pp_rep(validator, view_no):
    assert validator.validate_old_view_prep_prepare_rep(old_view_pp_rep()) == (PROCESS, None)


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
    validator._data.last_ordered_3pc = (0, 1)
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


def test_discard_incorrect_inst_id(validator, view_no):
    inst_id = validator._data.inst_id + 1
    assert validator.validate_pre_prepare(pre_prepare(view_no, 1, inst_id)) == (DISCARD, INCORRECT_INSTANCE)
    assert validator.validate_prepare(prepare(view_no, 1, inst_id)) == (DISCARD, INCORRECT_INSTANCE)
    assert validator.validate_commit(commit(view_no, 1, inst_id)) == (DISCARD, INCORRECT_INSTANCE)


@pytest.mark.parametrize('pp_seq_no', [1, 9, 10, 11])
def test_process_ordered_pre_prepare(validator, view_no, pp_seq_no):
    validator._data.last_ordered_3pc = (view_no, 10)
    validator._data.prev_view_prepare_cert = 10
    assert validator.validate_pre_prepare(pre_prepare(view_no, pp_seq_no)) == (PROCESS, None)


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
    assert validator.validate_old_view_prep_prepare_req(old_view_pp_req()) == result
    assert validator.validate_old_view_prep_prepare_rep(old_view_pp_rep()) == result


def test_stash_future_view(validator, view_no):
    assert validator.validate_pre_prepare(pre_prepare(view_no + 1, 1)) == (STASH_VIEW_3PC, FUTURE_VIEW)
    assert validator.validate_prepare(prepare(view_no + 1, 1)) == (STASH_VIEW_3PC, FUTURE_VIEW)
    assert validator.validate_commit(commit(view_no + 1, 1)) == (STASH_VIEW_3PC, FUTURE_VIEW)
    assert validator.validate_new_view(new_view(view_no + 1)) == (STASH_VIEW_3PC, FUTURE_VIEW)

    assert validator.validate_pre_prepare(pre_prepare(view_no + 2, 1)) == (STASH_VIEW_3PC, FUTURE_VIEW)
    assert validator.validate_prepare(prepare(view_no + 2, 1)) == (STASH_VIEW_3PC, FUTURE_VIEW)
    assert validator.validate_commit(commit(view_no + 2, 1)) == (STASH_VIEW_3PC, FUTURE_VIEW)
    assert validator.validate_new_view(new_view(view_no + 2)) == (STASH_VIEW_3PC, FUTURE_VIEW)


def test_stash_waiting_for_new_view_3pc(validator, view_no):
    validator._data.waiting_for_new_view = True
    assert validator.validate_pre_prepare(pre_prepare(view_no, 1)) == (STASH_VIEW_3PC, WAITING_FOR_NEW_VIEW)
    assert validator.validate_prepare(prepare(view_no, 1)) == (STASH_VIEW_3PC, WAITING_FOR_NEW_VIEW)
    assert validator.validate_commit(commit(view_no, 1)) == (STASH_VIEW_3PC, WAITING_FOR_NEW_VIEW)


def test_stash_waiting_for_new_view_old_view_pp_rep(validator, view_no):
    validator._data.waiting_for_new_view = True
    assert validator.validate_old_view_prep_prepare_rep(old_view_pp_rep()) == (
        STASH_VIEW_3PC, WAITING_FOR_NEW_VIEW)


def test_process_waiting_for_new_view_old_view_pp_req(validator, view_no):
    validator._data.waiting_for_new_view = True
    assert validator.validate_old_view_prep_prepare_req(old_view_pp_req()) == (PROCESS, None)


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
    validator._data.last_ordered_3pc = (0, 1)
    validator._data.low_watermark = 100
    validator._data.high_watermark = 400
    assert validator.validate_pre_prepare(pre_prepare(view_no, pp_seq_no)) == result
    assert validator.validate_prepare(prepare(view_no, pp_seq_no)) == result
    assert validator.validate_commit(commit(view_no, pp_seq_no)) == result


def test_process_waiting_for_new_view_new_view(validator, view_no):
    validator._data.waiting_for_new_view = True
    assert validator.validate_new_view(new_view(view_no)) == (PROCESS, None)


@pytest.mark.parametrize('pp_seq_no', [1, 9, 10, 11])
def test_process_ordered_prepare_commit(validator, view_no, pp_seq_no):
    validator._data.last_ordered_3pc = (view_no, 10)
    validator._data.prev_view_prepare_cert = 10
    assert validator.validate_prepare(prepare(view_no, pp_seq_no)) == (PROCESS, None)
    assert validator.validate_commit(commit(view_no, pp_seq_no)) == (PROCESS, None)


def test_discard_non_master_old_view_pp_req(validator):
    old_view_pp_req_msg = old_view_pp_req()
    validator._data.is_master = False
    validator._data.inst_id = 1
    old_view_pp_req_msg.instId = validator._data.inst_id
    assert validator.validate_old_view_prep_prepare_req(old_view_pp_req_msg) == (DISCARD, NON_MASTER)


def test_discard_non_master_old_view_pp_rep(validator):
    old_view_pp_rep_msg = old_view_pp_rep()
    validator._data.is_master = False
    validator._data.inst_id = 1
    old_view_pp_rep_msg.instId = validator._data.inst_id
    assert validator.validate_old_view_prep_prepare_rep(old_view_pp_rep_msg) == (DISCARD, NON_MASTER)


def test_discard_old_view_pp_req_with_incorrect_inst_id(validator):
    old_view_pp_req_msg = old_view_pp_req()
    old_view_pp_req_msg.instId = validator._data.inst_id + 1
    assert validator.validate_old_view_prep_prepare_req(old_view_pp_req_msg) == (DISCARD, INCORRECT_INSTANCE)


def test_discard_old_view_pp_rep_with_incorrect_inst_id(validator):
    old_view_pp_rep_msg = old_view_pp_rep()
    old_view_pp_rep_msg.instId = validator._data.inst_id + 1
    assert validator.validate_old_view_prep_prepare_rep(old_view_pp_rep_msg) == (DISCARD, INCORRECT_INSTANCE)


def test_process_non_master_new_view(validator, view_no):
    validator._data.is_master = False
    validator._data.inst_id = 1
    assert validator.validate_new_view(new_view(view_no)) == (PROCESS, None)


@pytest.mark.parametrize('pp_seq_no, result', [
    (10, (PROCESS, None)),
    (11, (PROCESS, None)),
    (12, (STASH_WAITING_FIRST_BATCH_IN_VIEW, WAITING_FIRST_BATCH_IN_VIEW)),
    (13, (STASH_WAITING_FIRST_BATCH_IN_VIEW, WAITING_FIRST_BATCH_IN_VIEW)),
    (100, (STASH_WAITING_FIRST_BATCH_IN_VIEW, WAITING_FIRST_BATCH_IN_VIEW)),
])
def test_stash_from_new_view_until_first_batch_is_ordered_non_zero_view(validator, pp_seq_no, result):
    validator._data.view_no = 1
    validator._data.prev_view_prepare_cert = 10
    validator._data.last_ordered_3pc = (1, 10)
    assert validator.validate_pre_prepare(pre_prepare(1, pp_seq_no)) == result
    assert validator.validate_prepare(prepare(1, pp_seq_no)) == result
    assert validator.validate_commit(commit(1, pp_seq_no)) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    (10, (PROCESS, None)),
    (11, (DISCARD, ALREADY_ORDERED)),
    (12, (PROCESS, None)),
    (13, (PROCESS, None)),
    (100, (PROCESS, None)),
])
def test_process_from_new_view_if_first_batch_is_ordered_non_zero_view(validator, pp_seq_no, result):
    validator._data.view_no = 1
    validator._data.prev_view_prepare_cert = 10
    validator._data.last_ordered_3pc = (1, 11)
    assert validator.validate_pre_prepare(pre_prepare(1, pp_seq_no)) == result
    assert validator.validate_prepare(prepare(1, pp_seq_no)) == result
    assert validator.validate_commit(commit(1, pp_seq_no)) == result


@pytest.mark.parametrize('pp_seq_no, result', [
    (10, (PROCESS, None)),
    (11, (PROCESS, None)),
    (12, (PROCESS, None)),
    (13, (PROCESS, None)),
    (100, (PROCESS, None)),
])
def test_process_from_new_view_if_first_batch_not_ordered_zero_view(validator, pp_seq_no, result):
    validator._data.view_no = 0
    validator._data.prev_view_prepare_cert = 10
    validator._data.last_ordered_3pc = (0, 10)
    assert validator.validate_pre_prepare(pre_prepare(0, pp_seq_no)) == result
    assert validator.validate_prepare(prepare(0, pp_seq_no)) == result
    assert validator.validate_commit(commit(0, pp_seq_no)) == result

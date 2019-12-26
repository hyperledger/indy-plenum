from unittest.mock import Mock

import pytest

from common.exceptions import LogicError
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.internal_messages import CheckpointStabilized, NeedBackupCatchup, NeedMasterCatchup
from plenum.common.messages.node_messages import Checkpoint, Ordered, PrePrepare
from plenum.common.util import getMaxFailures
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.utils import preprepare_to_batch_id
from plenum.test.checkpoints.conftest import chkFreqPatched
from plenum.test.checkpoints.helper import cp_digest, cp_key
from plenum.test.helper import create_pre_prepare_params


@pytest.fixture
def pre_prepare(checkpoint_service):
    params = create_pre_prepare_params(None,
                                       ledger_id=DOMAIN_LEDGER_ID,
                                       view_no=checkpoint_service.view_no,
                                       pp_seq_no=1)
    pp = PrePrepare(*params)
    return pp


@pytest.fixture
def ordered(pre_prepare):
    ord_args = [
        pre_prepare.instId,
        pre_prepare.viewNo,
        pre_prepare.reqIdr,
        [],
        pre_prepare.ppSeqNo,
        pre_prepare.ppTime,
        pre_prepare.ledgerId,
        pre_prepare.stateRootHash,
        pre_prepare.txnRootHash,
        pre_prepare.auditTxnRootHash,
        ["Alpha", "Beta"],
        ["Alpha", "Beta", "Gamma", "Delta"],
        pre_prepare.viewNo,
        pre_prepare.digest
    ]
    return Ordered(*ord_args)


@pytest.fixture
def checkpoint(ordered, tconf):
    start = ordered.ppSeqNo % tconf.CHK_FREQ
    return Checkpoint(instId=ordered.instId,
                      viewNo=ordered.viewNo,
                      seqNoStart=0,
                      seqNoEnd=start + tconf.CHK_FREQ - 1,
                      digest=cp_digest(start + tconf.CHK_FREQ - 1))


# TODO: Add test checking that our checkpoint is stabilized only if we receive
#  quorum of checkpoints with expected digest

def test_start_catchup_on_quorum_of_stashed_checkpoints(checkpoint_service, checkpoint, pre_prepare,
                                                        tconf, ordered, validators, is_master):
    master_catchup_handler = Mock()
    backup_catchup_handler = Mock()
    checkpoint_service._bus.subscribe(NeedMasterCatchup, master_catchup_handler)
    checkpoint_service._bus.subscribe(NeedBackupCatchup, backup_catchup_handler)

    def check_catchup_not_started():
        master_catchup_handler.assert_not_called()
        backup_catchup_handler.assert_not_called()

    def check_catchup_started(till_seq_no: int):
        if is_master:
            master_catchup_handler.assert_called_once_with(NeedMasterCatchup())
            backup_catchup_handler.assert_not_called()
        else:
            master_catchup_handler.assert_not_called()
            backup_catchup_handler.assert_called_once_with(
                NeedBackupCatchup(inst_id=checkpoint_service._data.inst_id,
                                  caught_up_till_3pc=(checkpoint_service.view_no,
                                                      till_seq_no)))

    quorum = checkpoint_service._data.quorums.checkpoint.value
    n = len(validators)
    assert quorum == n - getMaxFailures(n) - 1
    senders = ["sender{}".format(i) for i in range(quorum + 1)]

    till_seq_no = 2 * tconf.CHK_FREQ

    new_checkpoint = Checkpoint(instId=ordered.instId,
                                viewNo=ordered.viewNo,
                                seqNoStart=0,
                                seqNoEnd=till_seq_no,
                                digest=cp_digest(till_seq_no))

    key = checkpoint_service._checkpoint_key(checkpoint)
    for sender in senders[:quorum]:
        checkpoint_service.process_checkpoint(checkpoint, sender)
        assert sender in checkpoint_service._received_checkpoints[key]
    check_catchup_not_started()

    new_key = checkpoint_service._checkpoint_key(new_checkpoint)
    for sender in senders[:quorum - 1]:
        checkpoint_service.process_checkpoint(new_checkpoint, sender)
        assert sender in checkpoint_service._received_checkpoints[new_key]
    check_catchup_not_started()

    checkpoint_service.process_checkpoint(new_checkpoint, senders[quorum - 1])
    check_catchup_started(till_seq_no)


def test_process_backup_catchup_msg(checkpoint_service, tconf, checkpoint):
    till_seq_no = tconf.CHK_FREQ
    key = cp_key(checkpoint.viewNo, till_seq_no)

    new_till_seq_no = till_seq_no = tconf.CHK_FREQ
    new_key = cp_key(checkpoint.viewNo, new_till_seq_no)

    checkpoint_service._data.last_ordered_3pc = (checkpoint_service.view_no, 0)
    checkpoint_service._data.stable_checkpoint = 1

    checkpoint_service._received_checkpoints[key] = {"frm"}
    checkpoint_service._received_checkpoints[new_key] = {"frm"}
    checkpoint_service._data.checkpoints.append(checkpoint)

    checkpoint_service._data.last_ordered_3pc = (checkpoint_service.view_no, till_seq_no)
    checkpoint_service.caught_up_till_3pc(checkpoint_service._data.last_ordered_3pc)

    assert checkpoint_service._data.low_watermark == till_seq_no
    assert checkpoint in checkpoint_service._data.checkpoints
    assert checkpoint_service._data.stable_checkpoint == till_seq_no
    assert key not in checkpoint_service._received_checkpoints
    assert new_key not in checkpoint_service._received_checkpoints


def test_process_checkpoint(checkpoint_service, checkpoint, pre_prepare, tconf, ordered, validators, is_master):
    checkpoint_stabilized_handler = Mock()
    checkpoint_service._bus.subscribe(CheckpointStabilized, checkpoint_stabilized_handler)
    quorum = checkpoint_service._data.quorums.checkpoint.value
    n = len(validators)
    assert quorum == n - getMaxFailures(n) - 1
    senders = ["sender{}".format(i) for i in range(quorum + 1)]

    till_seq_no = tconf.CHK_FREQ

    checkpoint_service._received_checkpoints[cp_key(checkpoint.viewNo, 1)] = {"frm"}
    # For now, on checkpoint stabilization phase all checkpoints
    # with ppSeqNo less then stable_checkpoint will be removed
    checkpoint_service._received_checkpoints[cp_key(checkpoint.viewNo + 1, till_seq_no + 100)] = {"frm"}

    pre_prepare.ppSeqNo = till_seq_no
    pre_prepare.auditTxnRootHash = cp_digest(till_seq_no)
    ordered.ppSeqNo = pre_prepare.ppSeqNo
    ordered.auditTxnRootHash = pre_prepare.auditTxnRootHash
    checkpoint_service._data.preprepared.append(preprepare_to_batch_id(pre_prepare))
    checkpoint_service.process_ordered(ordered)

    _check_checkpoint(checkpoint_service, till_seq_no, pre_prepare, check_shared_data=True)

    for sender in senders[:quorum - 1]:
        checkpoint_service.process_checkpoint(checkpoint, sender)
    assert checkpoint_service._data.stable_checkpoint < till_seq_no

    # send the last checkpoint to stable it
    checkpoint_service.process_checkpoint(checkpoint, senders[quorum - 1])
    assert checkpoint_service._data.stable_checkpoint == till_seq_no

    # check _remove_stashed_checkpoints()
    assert sum(1 for cp in checkpoint_service._received_checkpoints
               if cp.view_no == checkpoint.viewNo) == 0
    assert sum(1 for cp in checkpoint_service._received_checkpoints if
               cp.view_no == checkpoint.viewNo + 1) > 0

    # check watermarks
    assert checkpoint_service._data.low_watermark == checkpoint.seqNoEnd

    # check that a Cleanup msg has been sent
    checkpoint_stabilized_handler.assert_called_once_with(
        CheckpointStabilized(last_stable_3pc=(checkpoint.viewNo, checkpoint.seqNoEnd)))


def test_process_ordered(checkpoint_service, ordered, pre_prepare, tconf):
    with pytest.raises(LogicError, match="CheckpointService | Can't process Ordered msg because "
                                         "ppSeqNo {} not in preprepared".format(ordered.ppSeqNo)):
        checkpoint_service.process_ordered(ordered)

    checkpoint_service._data.preprepared.append(preprepare_to_batch_id(pre_prepare))
    checkpoint_service.process_ordered(ordered)
    _check_checkpoint(checkpoint_service, tconf.CHK_FREQ, pre_prepare)

    pre_prepare.ppSeqNo = tconf.CHK_FREQ
    ordered.ppSeqNo = pre_prepare.ppSeqNo
    checkpoint_service._data.preprepared.append(preprepare_to_batch_id(pre_prepare))
    checkpoint_service.process_ordered(ordered)
    _check_checkpoint(checkpoint_service, tconf.CHK_FREQ, pre_prepare, check_shared_data=True)

    pre_prepare.ppSeqNo += 1
    ordered.ppSeqNo = pre_prepare.ppSeqNo
    checkpoint_service._data.preprepared.append(preprepare_to_batch_id(pre_prepare))
    checkpoint_service.process_ordered(ordered)
    _check_checkpoint(checkpoint_service, tconf.CHK_FREQ * 2, pre_prepare)


def _check_checkpoint(checkpoint_service: CheckpointService,
                      cp_seq_no: int,
                      pp: PrePrepare,
                      check_shared_data: bool = False):
    for checkpoint in checkpoint_service._data.checkpoints:
        if checkpoint.seqNoEnd == cp_seq_no:
            assert checkpoint.instId == pp.instId
            assert checkpoint.viewNo == pp.viewNo
            if pp.ppSeqNo == cp_seq_no:
                assert checkpoint.digest == pp.auditTxnRootHash
            return
    assert not check_shared_data, "The checkpoint should contains in the consensus_data."


def test_remove_stashed_checkpoints_doesnt_crash_when_current_view_no_is_greater_than_last_stashed_checkpoint(
        checkpoint_service):
    till_3pc_key = (1, 1)
    checkpoint_service._received_checkpoints[cp_key(1, 1)] = {"some_node", "other_node"}
    checkpoint_service._data.view_no = 2
    checkpoint_service._remove_received_checkpoints(till_3pc_key)
    assert not checkpoint_service._received_checkpoints


@pytest.fixture(params=["current_view", "future_view"])
def caughtup_view_no(request, initial_view_no):
    if request.param == "current_view":
        return initial_view_no
    return initial_view_no + 1


CHK_FREQ = 100


@pytest.mark.parametrize('caughtup_pp_seq_no, expected_stable_checkpoint', [
    (1, 0),
    (99, 0),
    (100, 100),
    (101, 100),
    (199, 100),
    (200, 200),
    (1001, 1000)
])
def test_caught_up_till_3pc_stabilizes_checkpoint(checkpoint_service,
                                                  tconf, chkFreqPatched,
                                                  is_master, initial_view_no, caughtup_view_no,
                                                  caughtup_pp_seq_no, expected_stable_checkpoint):
    checkpoint_service._get_view_no_from_audit = lambda x: initial_view_no
    checkpoint_service._get_digest_from_audit = lambda x, y: cp_digest(9999)
    data = checkpoint_service._data
    last_ordered = (caughtup_view_no, caughtup_pp_seq_no)

    # emulate finish of catchup
    data.last_ordered_3pc = last_ordered
    data.view_no = last_ordered[0]

    # call caught_up_till_3pc
    checkpoint_service.caught_up_till_3pc(last_ordered)

    # stable checkpoint must be updated
    assert data.stable_checkpoint == expected_stable_checkpoint
    assert data.last_checkpoint.seqNoEnd == data.stable_checkpoint
    expected_view_no = initial_view_no if caughtup_pp_seq_no >= 100 else 0
    assert data.last_checkpoint.viewNo == expected_view_no
    expected_digest = cp_digest(9999) if caughtup_pp_seq_no >= 100 else None
    assert data.last_checkpoint.digest == expected_digest
    assert len(data.checkpoints) == 1

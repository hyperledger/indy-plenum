import pytest

from common.exceptions import LogicError
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Checkpoint, Ordered, PrePrepare, CheckpointState
from plenum.common.util import updateNamedTuple, getMaxFailures
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica import ConsensusDataHelper
from plenum.test.helper import create_pre_prepare_params


@pytest.fixture
def pre_prepare(checkpoint_service):
    params = create_pre_prepare_params(None,
                                       ledger_id=DOMAIN_LEDGER_ID,
                                       view_no=checkpoint_service.view_no,
                                       pp_seq_no=1)
    pp = PrePrepare(*params)
    # checkpoint_service._data.preprepared.append(pp)
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
        ["Alpha", "Beta"]
    ]
    return Ordered(*ord_args)


@pytest.fixture
def checkpoint(ordered, tconf):
    start = ordered.ppSeqNo % tconf.CHK_FREQ
    return Checkpoint(instId=ordered.instId,
                      viewNo=ordered.viewNo,
                      seqNoStart=start,
                      seqNoEnd=start + tconf.CHK_FREQ - 1,
                      digest='digest')


def test_process_checkpoint_stash_recvd(checkpoint_service, checkpoint, pre_prepare, tconf, ordered, validators):
    quorum = checkpoint_service._data.quorums.checkpoint.value
    print(quorum)
    n = len(validators)
    assert quorum == n - getMaxFailures(n) - 1
    senders = ["sender{}".format(i) for i in range(quorum + 1)]

    pre_prepare.ppSeqNo = tconf.CHK_FREQ
    ordered.ppSeqNo = pre_prepare.ppSeqNo
    checkpoint_service._data.preprepared.append(pre_prepare)
    checkpoint_service._checkpoint_state[1, tconf.CHK_FREQ] = CheckpointState(1, ["digest"] * (tconf.CHK_FREQ - 1), None, {}, False)
    checkpoint_service._bus.send(ordered)
    _check_checkpoint(checkpoint_service, 1, tconf.CHK_FREQ, pre_prepare, check_shared_data=True)

    key = (1, tconf.CHK_FREQ)

    for sender in senders[:quorum - 1]:
        print(sender)
        assert checkpoint_service.process_checkpoint(checkpoint, sender)
        print(checkpoint_service._stashed_recvd_checkpoints[checkpoint.viewNo].keys())
        assert list(checkpoint_service._stashed_recvd_checkpoints[checkpoint.viewNo].keys())[0] == key
        assert checkpoint_service._stashed_recvd_checkpoints[checkpoint.viewNo][key][sender] == checkpoint

    assert not checkpoint_service.process_checkpoint(checkpoint, senders[quorum - 1])
    assert checkpoint_service._checkpoint_state[key].isStable
    assert key not in checkpoint_service._stashed_recvd_checkpoints[checkpoint.viewNo]



def test_process_checkpoint(checkpoint_service, checkpoint, pre_prepare, tconf, ordered, validators, is_master):
    quorum = checkpoint_service._data.quorums.checkpoint.value
    print(quorum)
    n = len(validators)
    assert quorum == n - getMaxFailures(n) - 1
    senders = ["sender{}".format(i) for i in range(quorum + 1)]

    checkpoint_service._stash_checkpoint(Checkpoint(1, checkpoint.viewNo, 1, 1, "1"), "frm")
    checkpoint_service._stash_checkpoint(Checkpoint(1, checkpoint.viewNo + 1, 1, 1, "1"), "frm")


    checkpoint_service._checkpoint_state[1, tconf.CHK_FREQ] = CheckpointState(1, ["digest"] * (tconf.CHK_FREQ - 1), None, {}, False)
    pre_prepare.ppSeqNo = tconf.CHK_FREQ
    ordered.ppSeqNo = pre_prepare.ppSeqNo
    checkpoint_service._data.preprepared.append(pre_prepare)
    checkpoint_service._bus.send(ordered)
    _check_checkpoint(checkpoint_service, 1, tconf.CHK_FREQ, pre_prepare, check_shared_data=True)
    state = updateNamedTuple(checkpoint_service._checkpoint_state[1, tconf.CHK_FREQ],
                             digest=checkpoint.digest)
    checkpoint_service._checkpoint_state[1, tconf.CHK_FREQ] = state

    key = (1, tconf.CHK_FREQ)

    assert checkpoint.viewNo + 1 in checkpoint_service._stashed_recvd_checkpoints
    for sender in senders[:quorum - 1]:
        print(sender)
        assert checkpoint_service.process_checkpoint(checkpoint, sender)
        assert checkpoint_service._checkpoint_state[key].receivedDigests[sender] == checkpoint.digest

    assert not checkpoint_service._checkpoint_state[key].isStable
    assert checkpoint_service.process_checkpoint(checkpoint, senders[quorum - 1])
    assert checkpoint_service._checkpoint_state[key].isStable

    # check _remove_stashed_checkpoints()
    assert checkpoint.viewNo not in checkpoint_service._stashed_recvd_checkpoints
    assert checkpoint.viewNo + 1 in checkpoint_service._stashed_recvd_checkpoints

    # check watermarks
    assert checkpoint_service._data.low_watermark == checkpoint.seqNoEnd


def test_process_oredered(checkpoint_service, ordered, pre_prepare, tconf):
    with pytest.raises(LogicError, match="CheckpointService | Can't process Ordered msg because "
                                         "ppSeqNo {} not in preprepared".format(ordered.ppSeqNo)):
        checkpoint_service._bus.send(ordered)
    checkpoint_service._data.preprepared.append(pre_prepare)
    checkpoint_service._bus.send(ordered)
    _check_checkpoint(checkpoint_service, 1, tconf.CHK_FREQ, pre_prepare)

    pre_prepare.ppSeqNo = tconf.CHK_FREQ
    ordered.ppSeqNo = pre_prepare.ppSeqNo
    checkpoint_service._data.preprepared.append(pre_prepare)
    state = updateNamedTuple(checkpoint_service._checkpoint_state[1, tconf.CHK_FREQ],
                             digests=["digest"] * (tconf.CHK_FREQ - 1))
    checkpoint_service._checkpoint_state[1, tconf.CHK_FREQ] = state
    checkpoint_service._bus.send(ordered)
    _check_checkpoint(checkpoint_service, 1, tconf.CHK_FREQ, pre_prepare, check_shared_data=True)

    pre_prepare.ppSeqNo += 1
    ordered.ppSeqNo = pre_prepare.ppSeqNo
    checkpoint_service._data.preprepared.append(pre_prepare)
    checkpoint_service._bus.send(ordered)
    _check_checkpoint(checkpoint_service, tconf.CHK_FREQ + 1, tconf.CHK_FREQ * 2, pre_prepare)


def _check_checkpoint(checkpoint_service: CheckpointService, start, end, pp,
                      check_shared_data=False):
    assert (start, end) in checkpoint_service._checkpoint_state
    assert checkpoint_service._checkpoint_state[(start, end)].seqNo == pp.ppSeqNo
    assert (pp.digest in checkpoint_service._checkpoint_state[(start, end)].digests) or \
           checkpoint_service._checkpoint_state[(start, end)].digest

    for checkpoint in checkpoint_service._data.checkpoints:
        if checkpoint.seqNoEnd == end and checkpoint.seqNoStart == start:
            assert checkpoint.instId == pp.instId
            assert checkpoint.viewNo == pp.viewNo
            assert checkpoint.digest
            return
    assert not check_shared_data, "The checkpoint should contains in the consensus_data."


def test_remove_stashed_checkpoints_doesnt_crash_when_current_view_no_is_greater_than_last_stashed_checkpoint(
        checkpoint_service):
    till_3pc_key = (1, 1)
    checkpoint_service._stashed_recvd_checkpoints[1] = {till_3pc_key: {}}
    checkpoint_service._data.view_no = 2
    # This shouldn't crash
    checkpoint_service._remove_stashed_checkpoints(till_3pc_key)
    assert not checkpoint_service._stashed_recvd_checkpoints

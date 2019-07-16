import pytest
from copy import deepcopy

from plenum.common.messages.node_messages import Checkpoint

from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica import ConsensusDataHelper

from plenum.test.consensus.conftest import pre_prepare, validators


@pytest.fixture
def consensus_data_helper(validators):
    return ConsensusDataHelper(ConsensusSharedData('sample', validators, 0))


@pytest.fixture
def checkpoint():
    return Checkpoint(instId=0,
                      viewNo=0,
                      seqNoStart=1,
                      seqNoEnd=100,
                      digest='digest')


def test_pp_storages_ordering(pre_prepare, consensus_data_helper: ConsensusDataHelper):
    consensus_data_helper.preprepare_batch(pre_prepare)
    assert consensus_data_helper.consensus_data.preprepared
    assert not consensus_data_helper.consensus_data.prepared

    consensus_data_helper.prepare_batch(pre_prepare)
    assert consensus_data_helper.consensus_data.preprepared
    assert consensus_data_helper.consensus_data.prepared

    consensus_data_helper.clear_batch(pre_prepare)
    assert not consensus_data_helper.consensus_data.preprepared
    assert not consensus_data_helper.consensus_data.prepared


def test_pp_storages_freeing(pre_prepare, consensus_data_helper: ConsensusDataHelper):
    consensus_data_helper.consensus_data.prepared.append(pre_prepare)
    consensus_data_helper.consensus_data.preprepared.append(pre_prepare)
    assert consensus_data_helper.consensus_data.preprepared
    assert consensus_data_helper.consensus_data.prepared
    consensus_data_helper.clear_all_batches()
    assert not consensus_data_helper.consensus_data.preprepared
    assert not consensus_data_helper.consensus_data.prepared


def test_pp_storages_freeing_till(pre_prepare, consensus_data_helper: ConsensusDataHelper):
    pre_prepare.ppSeqNo = 3
    consensus_data_helper.consensus_data.prepared.append(pre_prepare)
    consensus_data_helper.consensus_data.preprepared.append(pre_prepare)
    assert consensus_data_helper.consensus_data.preprepared
    assert consensus_data_helper.consensus_data.prepared
    consensus_data_helper.clear_batch_till_seq_no(4)
    assert not consensus_data_helper.consensus_data.preprepared
    assert not consensus_data_helper.consensus_data.prepared

    pre_prepare.ppSeqNo = 4
    consensus_data_helper.consensus_data.prepared.append(pre_prepare)
    consensus_data_helper.consensus_data.preprepared.append(pre_prepare)
    consensus_data_helper.clear_batch_till_seq_no(4)
    assert consensus_data_helper.consensus_data.preprepared
    assert consensus_data_helper.consensus_data.prepared


def test_checkpoint_storage(checkpoint, consensus_data_helper: ConsensusDataHelper):
    consensus_data_helper.add_checkpoint(checkpoint)
    checkpoint_new = Checkpoint(instId=0,
                                viewNo=0,
                                seqNoStart=2,
                                seqNoEnd=101,
                                digest='digest')
    consensus_data_helper.add_checkpoint(checkpoint_new)
    assert checkpoint in consensus_data_helper.consensus_data.checkpoints
    consensus_data_helper.set_stable_checkpoint(checkpoint_new.seqNoEnd)
    assert checkpoint not in consensus_data_helper.consensus_data.checkpoints
    assert checkpoint_new in consensus_data_helper.consensus_data.checkpoints

    consensus_data_helper.add_checkpoint(checkpoint)
    assert checkpoint in consensus_data_helper.consensus_data.checkpoints
    consensus_data_helper.reset_checkpoints()
    assert checkpoint not in consensus_data_helper.consensus_data.checkpoints

    consensus_data_helper.add_checkpoint(checkpoint)
    assert checkpoint in consensus_data_helper.consensus_data.checkpoints
    assert consensus_data_helper.consensus_data.stable_checkpoint == 0
    consensus_data_helper.set_stable_checkpoint(checkpoint.seqNoEnd)
    assert consensus_data_helper.consensus_data.stable_checkpoint == checkpoint.seqNoEnd

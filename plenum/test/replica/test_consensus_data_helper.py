import pytest

from plenum.common.messages.node_messages import Checkpoint

from plenum.server.consensus.consensus_shared_data import ConsensusSharedData, preprepare_to_batch_id
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
    consensus_data_helper.consensus_data.prepared.append(preprepare_to_batch_id(pre_prepare))
    consensus_data_helper.consensus_data.preprepared.append(preprepare_to_batch_id(pre_prepare))
    assert consensus_data_helper.consensus_data.preprepared
    assert consensus_data_helper.consensus_data.prepared
    consensus_data_helper.clear_all_batches()
    assert not consensus_data_helper.consensus_data.preprepared
    assert not consensus_data_helper.consensus_data.prepared


def test_pp_storages_freeing_till(pre_prepare, consensus_data_helper: ConsensusDataHelper):
    pre_prepare.ppSeqNo = 3
    consensus_data_helper.consensus_data.prepared.append(preprepare_to_batch_id(pre_prepare))
    consensus_data_helper.consensus_data.preprepared.append(preprepare_to_batch_id(pre_prepare))
    assert consensus_data_helper.consensus_data.preprepared
    assert consensus_data_helper.consensus_data.prepared
    consensus_data_helper.clear_batch_till_seq_no(4)
    assert not consensus_data_helper.consensus_data.preprepared
    assert not consensus_data_helper.consensus_data.prepared

    pre_prepare.ppSeqNo = 4
    consensus_data_helper.consensus_data.prepared.append(preprepare_to_batch_id(pre_prepare))
    consensus_data_helper.consensus_data.preprepared.append(preprepare_to_batch_id(pre_prepare))
    consensus_data_helper.clear_batch_till_seq_no(4)
    assert consensus_data_helper.consensus_data.preprepared
    assert consensus_data_helper.consensus_data.prepared

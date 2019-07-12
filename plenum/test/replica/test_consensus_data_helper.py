import pytest

from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica import ConsensusDataHelper

from plenum.test.consensus.conftest import pre_prepare


@pytest.fixture
def consensus_data_helper():
    return ConsensusDataHelper(ConsensusSharedData('sample', 0))


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

# def test_checkpoint_storage()

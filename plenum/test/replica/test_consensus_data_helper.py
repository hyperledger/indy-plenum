import pytest

from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica import ConsensusDataHelper

from plenum.test.consensus.conftest import pre_prepare
from plenum.test.consensus.conftest import validators


@pytest.fixture
def consensus_data_helper(validators):
    return ConsensusDataHelper(ConsensusSharedData('sample', validators, 0))


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


def test_pp_storages_freeing(pre_prepare, consensus_data_helper):
    consensus_data_helper.consensus_data.prepared.append(pre_prepare)
    consensus_data_helper.consensus_data.preprepared.append(pre_prepare)
    assert consensus_data_helper.consensus_data.preprepared
    assert consensus_data_helper.consensus_data.prepared
    consensus_data_helper.clear_all_batches()
    assert not consensus_data_helper.consensus_data.preprepared
    assert not consensus_data_helper.consensus_data.prepared

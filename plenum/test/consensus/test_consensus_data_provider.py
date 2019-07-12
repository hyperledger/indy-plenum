from plenum.common.util import SortedDict

from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica import ConsensusDataHelper


def test_initial_consensus_state(some_item, other_item, validators):
    name = some_item(validators)
    primary = other_item(validators)
    data = ConsensusSharedData(name, validators, 0)
    data.set_validators(validators)

    # General info
    assert data.name == name

    # Validators
    assert data.validators == validators
    assert data.quorums.n == len(validators)
    assert data.primary_name == None

    # View
    assert data.view_no == 0
    assert not data.waiting_for_new_view

    # 3PC votes
    assert data.preprepared == []
    assert data.prepared == []

    # Checkpoints
    assert data.stable_checkpoint == None
    assert list(data.checkpoints) == []


def test_pp_storages_ordering(pre_prepare, consensus_data_helper: ConsensusDataHelper):
    consensus_data_helper.preprepare_batch(pre_prepare)
    assert consensus_data_helper.consensus_data.preprepared
    assert not consensus_data_helper.consensus_data.prepared

    consensus_data_helper.prepare_batch(pre_prepare)
    assert not consensus_data_helper.consensus_data.preprepared
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


def test_checkpoint_storages(consensus_data_helper):
    pass

from plenum.common.util import SortedDict

from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider


def test_initial_consensus_state(some_item, other_item, validators):
    name = some_item(validators)
    primary = other_item(validators)
    data = ConsensusDataProvider(name, validators)
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


def test_pp_storages_ordering(pre_prepare, provider):
    provider.preprepare_batch(pre_prepare)
    assert provider.preprepared
    assert not provider.prepared

    provider.prepare_batch(pre_prepare)
    assert not provider.preprepared
    assert provider.prepared

    provider.clear_batch(pre_prepare)
    assert not provider.preprepared
    assert not provider.prepared


def test_pp_storages_freeing(pre_prepare, provider):
    provider.prepared.append(pre_prepare)
    provider.preprepared.append(pre_prepare)
    assert provider.preprepared
    assert provider.prepared
    provider.clear_all_batches()
    assert not provider.preprepared
    assert not provider.prepared


def test_checkpoint_storages(provider):
    pass

from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider


def test_initial_consensus_state(some_item, other_item, validators):
    name = some_item(validators)
    primary = other_item(validators)
    data = ConsensusDataProvider(name)
    data.set_validators(validators)
    data.set_primary_name(primary)

    # General info
    assert data.name == name

    # Validators
    assert data.validators == validators
    assert data.quorums.n == len(validators)
    assert data.primary_name == primary

    # View
    assert data.view_no == 0
    assert not data.waiting_for_new_view

    # 3PC votes
    assert data.preprepared == []
    assert data.prepared == []

    # Checkpoints
    assert data.stable_checkpoint == 0
    assert data.checkpoints == []


def test_pp_storages(pre_prepare):
    provider = ConsensusDataProvider('sample')

    def preprepared_empty():
        return not provider.preprepared

    def prepared_empty():
        return not provider.prepared

    provider.preprepare_batch(pre_prepare)
    assert not preprepared_empty()
    assert prepared_empty()

    provider.prepare_batch(pre_prepare)
    assert preprepared_empty()
    assert not prepared_empty()

    provider.free_batch(pre_prepare)
    assert preprepared_empty()
    assert prepared_empty()

    provider.prepared.append(pre_prepare)
    provider.preprepared.append(pre_prepare)
    assert not preprepared_empty()
    assert not prepared_empty()
    provider.free_all()
    assert preprepared_empty()
    assert prepared_empty()

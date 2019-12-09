from plenum.server.consensus.consensus_shared_data import ConsensusSharedData


def test_initial_consensus_state(some_item, other_item, validators):
    name = some_item(validators)
    primary = other_item(validators)
    data = ConsensusSharedData(name, validators, 0)
    data.primary_name = primary
    data.set_validators(validators)

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
    assert list(data.checkpoints) == [data.initial_checkpoint]

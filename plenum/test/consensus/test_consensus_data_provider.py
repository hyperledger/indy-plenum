from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider


def test_initial_consensus_state(validators, some_validator):
    state = ConsensusDataProvider(some_validator, validators)

    # General info
    assert state.name == some_validator

    # Validators
    assert state.validators == validators
    assert state.quorums.n == len(validators)

    # View
    assert state.view_no == 0
    assert not state.waiting_for_new_view

    # 3PC votes
    assert state.preprepared == []
    assert state.prepared == []

    # Checkpoints
    assert state.stable_checkpoint == 0
    assert state.checkpoints == []


def test_consensus_primary_name(initial_view_no, some_consensus_data):
    assert some_consensus_data.primary_name() == some_consensus_data.primary_name(some_consensus_data.view_no)

    assert some_consensus_data.primary_name(initial_view_no) != some_consensus_data.primary_name(initial_view_no + 1)
    assert some_consensus_data.primary_name(initial_view_no) != some_consensus_data.primary_name(initial_view_no - 1)

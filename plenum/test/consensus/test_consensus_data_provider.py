from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider


def test_initial_3pc_state():
    state = ConsensusDataProvider('some_node')

    # General info
    assert state.name == 'some_node'

    # View
    assert state.view_no == 0
    assert not state.waiting_for_new_view

    # 3PC votes
    assert state.preprepared == []
    assert state.prepared == []

    # Checkpoints
    assert state.stable_checkpoint == 0
    assert state.checkpoints == []


def test_enter_next_view_increases_view_no_and_waits_for_new_view(initial_view_no, consensus_data):
    consensus_data.enter_next_view()

    assert consensus_data.view_no == initial_view_no + 1
    assert consensus_data.waiting_for_new_view

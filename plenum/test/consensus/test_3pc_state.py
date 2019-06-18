from plenum.server.consensus.three_pc_state import ThreePCState


def test_initial_3pc_state():
    state = ThreePCState()
    assert state.view_no == 0
    assert not state.waiting_for_new_view


def test_enter_next_view_increases_view_no_and_waits_for_new_view(initial_view_no, any_3pc_state):
    any_3pc_state.enter_next_view()

    assert any_3pc_state.view_no == initial_view_no + 1
    assert any_3pc_state.waiting_for_new_view

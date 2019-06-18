import pytest

from plenum.server.consensus.three_pc_state import ThreePCState
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.test.helper import MockNetwork


@pytest.fixture(params=[0, 1, 2, 5, 10])
def initial_view_no(request):
    return request.param


@pytest.fixture(params=[False, True])
def already_in_view_change(request):
    return request.param


@pytest.fixture
def any_3pc_state(initial_view_no, already_in_view_change):
    state = ThreePCState()
    state._view_no = initial_view_no
    state._waiting_for_new_view = already_in_view_change
    return state


@pytest.fixture
def mock_network():
    return MockNetwork()


@pytest.fixture
def view_change_service(any_3pc_state, mock_network):
    return ViewChangeService(any_3pc_state, mock_network)

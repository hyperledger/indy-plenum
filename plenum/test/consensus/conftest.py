import pytest

from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.test.helper import MockNetwork


@pytest.fixture(params=[0, 1, 2, 5, 10])
def initial_view_no(request):
    return request.param


@pytest.fixture(params=[False, True])
def already_in_view_change(request):
    return request.param


@pytest.fixture
def consensus_data(initial_view_no, already_in_view_change):
    data = ConsensusDataProvider('some_node')
    data.view_no = initial_view_no
    data.waiting_for_new_view = already_in_view_change
    return data


@pytest.fixture
def mock_network():
    return MockNetwork()


@pytest.fixture
def view_change_service(consensus_data, mock_network):
    return ViewChangeService(consensus_data, mock_network)

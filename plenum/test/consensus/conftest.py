import pytest

from plenum.common.event_bus import InternalBus
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockNetwork


@pytest.fixture(params=[genNodeNames(4), genNodeNames(6), genNodeNames(7)])
def validators(request):
    return request.param


@pytest.fixture(params=range(7))
def some_node_name(request, validators):
    return validators[request.param % len(validators)]


@pytest.fixture(params=range(6))
def other_node_name(request, validators, some_node_name):
    other_validators = [node for node in validators if node != some_node_name]
    return other_validators[request.param % len(other_validators)]


@pytest.fixture(params=[0, 1, 2, 5, 10])
def initial_view_no(request):
    return request.param


@pytest.fixture(params=[False, True])
def already_in_view_change(request):
    return request.param


@pytest.fixture
def some_consensus_data(some_node_name, validators, initial_view_no, already_in_view_change):
    data = ConsensusDataProvider(some_node_name, validators)
    data.view_no = initial_view_no
    data.waiting_for_new_view = already_in_view_change
    return data


@pytest.fixture
def some_node_network():
    return MockNetwork()


@pytest.fixture
def some_view_change_service(some_consensus_data, some_node_network):
    return ViewChangeService(some_consensus_data, InternalBus(), some_node_network)


@pytest.fixture
def other_consensus_data(other_node_name, validators, initial_view_no, already_in_view_change):
    data = ConsensusDataProvider(other_node_name, validators)
    data.view_no = initial_view_no
    data.waiting_for_new_view = already_in_view_change
    return data


@pytest.fixture
def other_node_network():
    return MockNetwork()


@pytest.fixture
def other_view_change_service(other_consensus_data, other_node_network):
    return ViewChangeService(other_consensus_data, InternalBus(), other_node_network)

import pytest

from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.test.greek import genNodeNames


@pytest.fixture(params=[4, 6, 7])
def validators(request):
    return genNodeNames(request.param)


@pytest.fixture(params=[0, 1, -1])
def some_validator(request, validators):
    return validators[request.param]


@pytest.fixture(params=[0, 1, -1])
def other_validator(request, validators, some_validator):
    other_validators = [node for node in validators if node != some_validator]
    return other_validators[request.param]


@pytest.fixture(params=[0, 1, 2])
def initial_view_no(request):
    return request.param


@pytest.fixture(params=[False, True])
def already_in_view_change(request):
    return request.param


@pytest.fixture
def some_consensus_data(some_validator, validators, initial_view_no, already_in_view_change):
    data = ConsensusDataProvider(some_validator, validators)
    data.view_no = initial_view_no
    data.waiting_for_new_view = already_in_view_change
    return data


@pytest.fixture
def other_consensus_data(other_validator, validators, initial_view_no, already_in_view_change):
    data = ConsensusDataProvider(other_validator, validators)
    data.view_no = initial_view_no
    data.waiting_for_new_view = already_in_view_change
    return data


@pytest.fixture
def primary_validator_in_next_view(initial_view_no, validators):
    data = ConsensusDataProvider(validators[0], validators)
    data.view_no = initial_view_no + 1
    return data.primary_name()


@pytest.fixture(params=[0, 1, -1])
def non_primary_validator_in_next_view(request, primary_validator_in_next_view, validators):
    non_primary_validators = [node for node in validators if node != primary_validator_in_next_view]
    return non_primary_validators[request.param]


@pytest.fixture
def consensus_data_of_primary_in_next_view(primary_validator_in_next_view, validators, initial_view_no):
    data = ConsensusDataProvider(primary_validator_in_next_view, validators)
    data.view_no = initial_view_no
    return data


@pytest.fixture
def consensus_data_of_non_primary_in_next_view(non_primary_validator_in_next_view, validators, initial_view_no):
    data = ConsensusDataProvider(non_primary_validator_in_next_view, validators)
    data.view_no = initial_view_no
    return data

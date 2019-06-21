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

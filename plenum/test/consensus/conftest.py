import pytest

from plenum.common.messages.node_messages import Checkpoint
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.test.greek import genNodeNames


@pytest.fixture(params=[4, 6, 7, 8])
def validators(request):
    return genNodeNames(request.param)


@pytest.fixture(params=[0, 1, 2])
def initial_view_no(request):
    return request.param


@pytest.fixture(params=[False, True])
def already_in_view_change(request):
    return request.param


@pytest.fixture
def primary(validators):
    def _primary_in_view(view_no):
        return ViewChangeService._find_primary(validators, view_no)
    return _primary_in_view


@pytest.fixture
def initial_checkpoints(initial_view_no):
    return [Checkpoint(instId=0, viewNo=initial_view_no, seqNoStart=0, seqNoEnd=0, digest='empty')]


@pytest.fixture
def consensus_data(validators, primary, initial_view_no, initial_checkpoints):
    def _data(name):
        data = ConsensusDataProvider(name, validators, primary(initial_view_no))
        data.view_no = initial_view_no
        data.checkpoints = initial_checkpoints
        return data
    return _data

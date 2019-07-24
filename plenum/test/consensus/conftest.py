import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.startable import Mode
from plenum.common.event_bus import InternalBus
from plenum.common.messages.node_messages import PrePrepare, ViewChange
from plenum.common.util import get_utc_epoch
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.common.messages.node_messages import Checkpoint
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer, MockNetwork


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
        data = ConsensusSharedData(name, validators, 0)
        data.view_no = initial_view_no
        data.checkpoints = initial_checkpoints
        return data

    return _data

@pytest.fixture
def view_change_service():
    data = ConsensusSharedData("some_name", genNodeNames(4), 0)
    return ViewChangeService(data, MockTimer(0), InternalBus(), MockNetwork())

@pytest.fixture
def pre_prepare():
    return PrePrepare(
        0,
        0,
        1,
        get_utc_epoch(),
        ['f99937241d4c891c08e92a3cc25966607315ca66b51827b170d492962d58a9be'],
        '[]',
        'f99937241d4c891c08e92a3cc25966607315ca66b51827b170d492962d58a9be',
        DOMAIN_LEDGER_ID,
        'CZecK1m7VYjSNCC7pGHj938DSW2tfbqoJp1bMJEtFqvG',
        '7WrAMboPTcMaQCU1raoj28vnhu2bPMMd2Lr9tEcsXeCJ',
        0,
        True
    )


@pytest.fixture(scope='function',
                params=[Mode.starting, Mode.discovering, Mode.discovered,
                        Mode.syncing, Mode.synced])
def mode_not_participating(request):
    return request.param


@pytest.fixture(scope='function',
                params=[Mode.starting, Mode.discovering, Mode.discovered,
                        Mode.syncing, Mode.synced, Mode.participating])
def mode(request):
    return request.param


@pytest.fixture
def view_change_message():
    def _view_change(view_no: int):
        vc = ViewChange(
            viewNo=view_no,
            stableCheckpoint=4,
            prepared=[],
            preprepared=[],
            checkpoints=[Checkpoint(instId=0, viewNo=view_no, seqNoStart=0, seqNoEnd=4, digest='some')]
        )
        return vc

    return _view_change

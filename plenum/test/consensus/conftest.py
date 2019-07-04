import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import get_utc_epoch

from plenum.common.messages.node_messages import PrePrepare
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
def consensus_data(validators, primary, initial_view_no):
    def _data(name):
        data = ConsensusDataProvider(name, validators, primary(initial_view_no))
        data.view_no = initial_view_no
        return data

    return _data


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

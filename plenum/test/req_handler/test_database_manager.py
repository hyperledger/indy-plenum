import pytest

from plenum.server.database_manager import DatabaseManager
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(params=[True, False])
def taa_required(request):
    return request.param


def test_taa_requeired(taa_required):
    lid = 1
    db_manager = DatabaseManager()
    db_manager.register_new_database(lid, FakeSomething(), FakeSomething(), taa_acceptance_required=taa_required)
    assert db_manager.is_taa_acceptance_required(lid) == taa_required

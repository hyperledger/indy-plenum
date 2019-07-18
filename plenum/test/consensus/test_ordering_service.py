import pytest
from mock import Mock

from crypto.bls import bls_bft_replica
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.timer import QueueTimer
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(params=[True, False])
def is_master(request):
    return request.param

@pytest.fixture()
def internal_bus():
    return InternalBus()

@pytest.fixture()
def external_bus():
    send_handler = Mock()
    return ExternalBus(send_handler=send_handler)

@pytest.fixture()
def bls_bft_replica():
    return FakeSomething()

@pytest.fixture()
def db_manager():
    return DatabaseManager()

@pytest.fixture()
def write_manager(db_manager):
    return WriteRequestManager(database_manager=db_manager)

@pytest.fixture()
def name():
    return "OrderingService"

@pytest.fixture()
def orderer(consensus_data, internal_bus, external_bus, name):
    orderer = OrderingService(data=consensus_data(name),
                              timer=QueueTimer(),
                              bus=internal_bus,
                              network=external_bus,
                              write_manager=write_manager,
                              bls_bft_replica=bls_bft_replica,
                              is_master=is_master)
    return orderer


def test_orderer(orderer):
    pass
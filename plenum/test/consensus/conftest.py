from functools import partial
from random import Random

import pytest

from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.constants import DOMAIN_LEDGER_ID, AUDIT_LEDGER_ID
from plenum.common.messages.internal_messages import RequestPropagates, PrimarySelected
from plenum.common.startable import Mode, Status
from plenum.common.messages.node_messages import PrePrepare, ViewChange
from plenum.common.stashing_router import StashingRouter
from plenum.common.util import get_utc_epoch
from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.common.messages.node_messages import Checkpoint
from plenum.server.consensus.monitoring.primary_connection_monitor_service import PrimaryConnectionMonitorService
from plenum.server.consensus.primary_selector import RoundRobinConstantNodesPrimariesSelector
from plenum.server.consensus.replica_service import ReplicaService
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.server.consensus.view_change_trigger_service import ViewChangeTriggerService
from plenum.server.database_manager import DatabaseManager
from plenum.server.replica_helper import generateName
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.test.checkpoints.helper import cp_digest
from plenum.test.consensus.helper import primary_in_view, create_test_write_req_manager
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer, MockNetwork, create_pool_txn_data, TestInternalBus
from plenum.test.simulation.sim_random import DefaultSimRandom
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(params=[4, 6, 7], ids=['4nodes', '6nodes', '7nodes'])
def validators(request):
    return genNodeNames(request.param)


@pytest.fixture(params=[0, 2], ids=['view=0', 'view=2'])
def initial_view_no(request):
    return request.param


@pytest.fixture(params=[False, True])
def already_in_view_change(request):
    return request.param


@pytest.fixture
def primary(validators):
    return partial(primary_in_view, validators)


@pytest.fixture
def consensus_data(validators, primary, initial_view_no, is_master):
    def _data(name):
        data = ConsensusSharedData(generateName(name, 0), validators, 0, is_master)
        data.view_no = initial_view_no
        return data

    return _data


@pytest.fixture
def timer():
    return MockTimer(0)


@pytest.fixture
def view_change_service(internal_bus, external_bus, timer, stasher, validators):
    # TODO: Use validators fixture
    data = ConsensusSharedData("some_name", genNodeNames(4), 0)
    primaries_selector = RoundRobinConstantNodesPrimariesSelector(validators)
    return ViewChangeService(data, timer, internal_bus, external_bus, stasher, primaries_selector)


@pytest.fixture
def view_change_trigger_service(internal_bus, external_bus, timer, stasher, validators):
    # TODO: Use validators fixture
    data = ConsensusSharedData("some_name", genNodeNames(4), 0)
    data.node_mode = Mode.participating
    data.node_status = Status.started
    return ViewChangeTriggerService(data=data,
                                    timer=timer,
                                    bus=internal_bus,
                                    network=external_bus,
                                    db_manager=DatabaseManager(),
                                    stasher=stasher,
                                    is_master_degraded=lambda: False)


@pytest.fixture
def primary_connection_monitor_service(internal_bus, external_bus, timer):
    # TODO: Use validators fixture
    nodes = genNodeNames(4)
    data = ConsensusSharedData("some_name", nodes, 0)
    data.node_mode = Mode.participating
    data.node_status = Status.started
    data.primary_name = nodes[0]
    service = PrimaryConnectionMonitorService(data=data,
                                              timer=timer,
                                              bus=internal_bus,
                                              network=external_bus)
    internal_bus.send(PrimarySelected())
    return service


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
            checkpoints=[Checkpoint(instId=0, viewNo=view_no, seqNoStart=0, seqNoEnd=4, digest=cp_digest(4))]
        )
        return vc

    return _view_change


@pytest.fixture(params=[True, False], ids=['master', 'non-master'])
def is_master(request):
    return request.param


@pytest.fixture()
def internal_bus():
    def rp_handler(ib, msg):
        ib.msgs.setdefault(type(msg), []).append(msg)

    ib = TestInternalBus()
    ib.msgs = {}
    ib.subscribe(RequestPropagates, rp_handler)
    return ib


@pytest.fixture()
def external_bus(validators):
    network = MockNetwork()
    network.update_connecteds(set(validators))
    return network


@pytest.fixture()
def bls_bft_replica():
    return FakeSomething(gc=lambda *args, **kwargs: True,
                         validate_pre_prepare=lambda *args, **kwargs: None,
                         update_prepare=lambda params, lid: params,
                         process_prepare=lambda *args, **kwargs: None,
                         process_pre_prepare=lambda *args, **kwargs: None,
                         validate_prepare=lambda *args, **kwargs: None,
                         validate_commit=lambda *args, **kwargs: None,
                         update_commit=lambda params, pre_prepare: params,
                         process_commit=lambda *args, **kwargs: None)


@pytest.fixture()
def db_manager():
    audit_ledger = FakeSomething(size=0,
                                 get_last_committed_txn=lambda: None)
    dbm = DatabaseManager()
    dbm.register_new_database(AUDIT_LEDGER_ID, audit_ledger)
    return dbm


@pytest.fixture()
def write_manager(db_manager):
    wrm = WriteRequestManager(database_manager=db_manager)
    wrm.node_reg_handler = NodeRegHandler(db_manager)
    return wrm


@pytest.fixture()
def stasher(internal_bus, external_bus):
    return StashingRouter(limit=100000, buses=[internal_bus, external_bus])


@pytest.fixture()
def replica_service(validators, initial_view_no, timer,
                    internal_bus, external_bus):
    genesis_txns = create_pool_txn_data(
        node_names=validators,
        crypto_factory=create_default_bls_crypto_factory(),
        get_free_port=lambda: 8090)['txns']
    write_manager = create_test_write_req_manager("Alpha", genesis_txns)

    replica = ReplicaService("Alpha:0",
                             validators, initial_view_no,
                             timer,
                             internal_bus,
                             external_bus,
                             write_manager=write_manager,
                             bls_bft_replica=FakeSomething(gc=lambda key: None))

    return replica


@pytest.fixture(params=Random().sample(range(1000000), 100))
def random(request):
    return DefaultSimRandom(request.param)

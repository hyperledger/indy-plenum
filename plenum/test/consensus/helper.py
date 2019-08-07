from typing import Optional, List

from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus
from plenum.server.consensus.replica_service import ReplicaService
from plenum.server.database_manager import DatabaseManager
from plenum.server.ledgers_bootstrap import LedgersBootstrap, IN_MEMORY_LOCATION
from plenum.server.node import Node
from plenum.server.request_managers.read_request_manager import ReadRequestManager
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom
from plenum.test.testing_utils import FakeSomething


class TestLedgersBootstrap(LedgersBootstrap):
    def create_bls_bft(self):
        return FakeSomething()
        # self.bls_key_register = bls_key_register
        # self.bls_crypto_signer = bls_crypto_signer
        # self.bls_crypto_verifier = bls_crypto_verifier
        # self.bls_store = bls_store

    def update_txn_with_extra_data(self, txn):
        return txn


def create_test_write_req_manager(name: str) -> WriteRequestManager:
    db_manager = DatabaseManager()
    write_manager = WriteRequestManager(db_manager)
    read_manager = ReadRequestManager()
    bootstrap = TestLedgersBootstrap(
        write_req_manager=write_manager,
        read_req_manager=read_manager,
        action_req_manager=FakeSomething(),
        name=name,
        config=getConfig(),
        data_location=IN_MEMORY_LOCATION,
        genesis_dir=IN_MEMORY_LOCATION,
        ledger_ids=Node.ledger_ids
    )
    bootstrap.init_ledgers()
    return write_manager


class SimPool:
    def __init__(self, node_count: int = 4, random: Optional[SimRandom] = None):
        self._random = random if random else DefaultSimRandom()
        self._timer = MockTimer()
        self._network = SimNetwork(self._timer, self._random)
        validators = genNodeNames(node_count)
        primary_name = validators[0]
        self._nodes = [ReplicaService(name, validators, primary_name,
                                      self._timer, InternalBus(), self.network.create_peer(name),
                                      write_manager=create_test_write_req_manager(name))
                       for name in validators]

    @property
    def timer(self) -> MockTimer:
        return self._timer

    @property
    def network(self) -> SimNetwork:
        return self._network

    @property
    def nodes(self) -> List[ReplicaService]:
        return self._nodes

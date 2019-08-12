from functools import partial
from typing import Optional, List

from crypto.bls.bls_bft import BlsBft
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.config_util import getConfig
from plenum.common.constants import NODE, NYM
from plenum.common.event_bus import InternalBus
from plenum.common.txn_util import get_type
from plenum.server.consensus.replica_service import ReplicaService
from plenum.server.database_manager import DatabaseManager
from plenum.server.ledgers_bootstrap import LedgersBootstrap
from plenum.server.node import Node
from plenum.server.request_managers.read_request_manager import ReadRequestManager
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer, create_pool_txn_data
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom
from plenum.test.testing_utils import FakeSomething


class TestLedgersBootstrap(LedgersBootstrap):
    def _create_bls_bft(self) -> BlsBft:
        # TODO: Create actual objects instead of fakes
        return BlsBft(
            bls_crypto_signer=FakeSomething(),
            bls_crypto_verifier=FakeSomething(),
            bls_key_register=FakeSomething(),
            bls_store=FakeSomething())

    def _update_txn_with_extra_data(self, txn):
        return txn


def create_test_write_req_manager(name: str, genesis_txns: List) -> WriteRequestManager:
    db_manager = DatabaseManager()
    write_manager = WriteRequestManager(db_manager)
    read_manager = ReadRequestManager()

    bootstrap = TestLedgersBootstrap(
        write_req_manager=write_manager,
        read_req_manager=read_manager,
        action_req_manager=FakeSomething(),
        name=name,
        config=getConfig(),
        ledger_ids=Node.ledger_ids
    )
    bootstrap.set_genesis_transactions(
        [txn for txn in genesis_txns if get_type(txn) == NODE],
        [txn for txn in genesis_txns if get_type(txn) == NYM]
    )
    bootstrap.init()

    return write_manager


class SimPool:
    def __init__(self, node_count: int = 4, random: Optional[SimRandom] = None):
        self._random = random if random else DefaultSimRandom()
        self._timer = MockTimer()
        self._network = SimNetwork(self._timer, self._random)
        validators = genNodeNames(node_count)
        primary_name = validators[0]

        genesis_txns = create_pool_txn_data(
            node_names=validators,
            crypto_factory=create_default_bls_crypto_factory(),
            get_free_port=partial(random.integer, 9000, 9999))['txns']

        self._nodes = [ReplicaService(name, validators, primary_name,
                                      self._timer, InternalBus(), self.network.create_peer(name),
                                      write_manager=create_test_write_req_manager(name, genesis_txns))
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

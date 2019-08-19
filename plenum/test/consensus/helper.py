from functools import partial
from operator import itemgetter
from typing import Dict, Type, List, Optional

from crypto.bls.bls_bft import BlsBft
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.config_util import getConfig
from plenum.common.constants import NODE, NYM
from plenum.common.event_bus import InternalBus
from plenum.common.messages.node_messages import Checkpoint, ViewChange, NewView, ViewChangeAck
from plenum.common.txn_util import get_type
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData, BatchID
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector
from plenum.server.consensus.replica_service import ReplicaService
from plenum.server.consensus.view_change_service import ViewChangeService, view_change_digest
from plenum.server.database_manager import DatabaseManager
from plenum.server.ledgers_bootstrap import LedgersBootstrap
from plenum.server.node import Node
from plenum.server.request_managers.read_request_manager import ReadRequestManager
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.test.checkpoints.helper import cp_digest
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer, create_pool_txn_data
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import DefaultSimRandom, SimRandom
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


VIEW_CHANGE_SERVICE_FIELDS = 'view_no', 'waiting_for_new_view', 'primaries'
ORDERING_SERVICE_FIELDS = 'last_ordered_3pc', 'preprepared', 'prepared'
CHECKPOINT_SERVICE_FIELDS = 'stable_checkpoint', 'checkpoints', 'low_watermark', 'high_watermark'

FIELDS = {ViewChangeService: VIEW_CHANGE_SERVICE_FIELDS,
          OrderingService: ORDERING_SERVICE_FIELDS,
          CheckpointService: CHECKPOINT_SERVICE_FIELDS}


def copy_shared_data(data: ConsensusSharedData) -> Dict:
    fields_to_check = VIEW_CHANGE_SERVICE_FIELDS + ORDERING_SERVICE_FIELDS + CHECKPOINT_SERVICE_FIELDS
    data_vars = vars(data)
    return {k: data_vars[k] for k in fields_to_check}


def check_service_changed_only_owned_fields_in_shared_data(service: Type,
                                                           data1: Dict, data2: Dict):
    changed_field = FIELDS[service]
    data1 = {k: v for k, v in data1.items() if k not in changed_field}
    data2 = {k: v for k, v in data2.items() if k not in changed_field}
    assert data1 == data2


def create_checkpoints(view_no):
    return [Checkpoint(instId=0, viewNo=view_no, seqNoStart=0, seqNoEnd=200, digest=cp_digest(0, 200))]


def create_batches(view_no):
    return [BatchID(view_no, 11, "d1"),
            BatchID(view_no, 12, "d2"),
            BatchID(view_no, 13, "d3")]


def create_view_change(initial_view_no, stable_cp=10, batches=None):
    if batches is None:
        batches = create_batches(initial_view_no)
    digest = cp_digest(0, stable_cp)
    cp = Checkpoint(instId=0, viewNo=initial_view_no, seqNoStart=0, seqNoEnd=stable_cp, digest=digest)
    return ViewChange(viewNo=initial_view_no + 1,
                      stableCheckpoint=stable_cp,
                      prepared=batches,
                      preprepared=batches,
                      checkpoints=[cp])


def create_new_view_from_vc(vc, validators, checkpoint=None, batches=None):
    vc_digest = view_change_digest(vc)
    vcs = [(node_name, vc_digest) for node_name in validators]
    checkpoint = checkpoint or vc.checkpoints[0]
    batches = batches or vc.prepared
    return NewView(vc.viewNo,
                   sorted(vcs, key=itemgetter(0)),
                   checkpoint,
                   batches)


def create_new_view(initial_view_no, stable_cp, validators=None):
    validators = validators or genNodeNames(4)
    batches = create_batches(initial_view_no)
    vc = create_view_change(initial_view_no, stable_cp, batches)
    return create_new_view_from_vc(vc, validators)


def create_view_change_acks(vc, vc_frm, senders):
    digest = view_change_digest(vc)
    senders = [name for name in senders if name != vc_frm]
    return [(ViewChangeAck(viewNo=vc.viewNo, name=vc_frm, digest=digest), ack_frm) for ack_frm in senders]


def primary_in_view(validators, view_no):
    f = (len(validators) - 1) // 3
    return RoundRobinPrimariesSelector().select_primaries(view_no=view_no, instance_count=f + 1,
                                                          validators=validators)[0]

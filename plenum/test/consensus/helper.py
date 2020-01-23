import copy
from functools import partial
from operator import itemgetter
from typing import Dict, Type, List, Optional

from common.exceptions import LogicError
from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_bft_replica import BlsBftReplica
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.batched import Batched
from plenum.common.config_util import getConfig
from plenum.common.constants import NODE, NYM, SEQ_NO_DB_LABEL
from plenum.common.event_bus import InternalBus
from plenum.common.message_processor import MessageProcessor
from plenum.common.messages.internal_messages import NeedViewChange, CatchupFinished
from plenum.common.messages.node_message_factory import node_message_factory
from plenum.common.messages.node_messages import Checkpoint, ViewChange, NewView, ViewChangeAck, PrePrepare, Prepare, \
    Commit, MessageRep, OldViewPrePrepareRequest, OldViewPrePrepareReply
from plenum.common.startable import Mode
from plenum.common.request import ReqKey
from plenum.common.txn_util import get_type
from plenum.common.util import getMaxFailures
from plenum.persistence.req_id_to_txn import ReqIdrToTxn
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.batch_id import BatchID
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.primary_selector import RoundRobinConstantNodesPrimariesSelector
from plenum.server.consensus.replica_service import ReplicaService, NeedAddNode, NeedRemoveNode
from plenum.server.consensus.utils import preprepare_to_batch_id, replica_name_to_node_name
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.server.consensus.view_change_storages import view_change_digest
from plenum.server.database_manager import DatabaseManager
from plenum.server.ledgers_bootstrap import LedgersBootstrap
from plenum.server.node import Node
from plenum.server.replica_helper import generateName
from plenum.server.request_managers.read_request_manager import ReadRequestManager
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.test.buy_handler import BuyHandler
from plenum.test.checkpoints.helper import cp_digest
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer, create_pool_txn_data, create_pre_prepare_no_bls, generate_state_root
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import DefaultSimRandom, SimRandom
from plenum.test.testing_utils import FakeSomething
from storage.kv_in_memory import KeyValueStorageInMemory
from stp_core.common.log import getlogger
from stp_zmq.zstack import ZStack


logger = getlogger()


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


def register_test_handler(wm):
    th = BuyHandler(wm.database_manager)
    wm.register_req_handler(th)


def create_test_write_req_manager(name: str, genesis_txns: List) -> WriteRequestManager:
    db_manager = DatabaseManager()
    write_manager = WriteRequestManager(db_manager)
    read_manager = ReadRequestManager()

    register_test_handler(write_manager)
    db_manager.register_new_store(SEQ_NO_DB_LABEL, ReqIdrToTxn(KeyValueStorageInMemory()))

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
    bootstrap.upload_states()

    return write_manager


class MockBlsBftReplica(BlsBftReplica):

    def __init__(self):
        super().__init__(bls_bft=None,
                         is_master=True)

    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        return None

    def validate_prepare(self, prepare: Prepare, sender):
        return None

    def validate_commit(self, commit: Commit, sender, pre_prepare: PrePrepare):
        return None

    def process_pre_prepare(self, pre_prepare: PrePrepare, sender):
        return True

    def process_prepare(self, prepare: Prepare, sender):
        return True

    def process_commit(self, commit: Commit, sender):
        return True

    def process_order(self, key, quorums, pre_prepare: PrePrepare):
        return True

    def update_pre_prepare(self, pre_prepare_params, ledger_id):
        return pre_prepare_params

    def update_prepare(self, prepare_params, ledger_id):
        return prepare_params

    def update_commit(self, commit_params, pre_prepare: PrePrepare):
        return commit_params

    def gc(self, key_3PC):
        pass


class SimPool:
    def __init__(self, node_count: int = 4, random: Optional[SimRandom] = None):
        self._initial_view_no = random.integer(0, 1000)
        self._random = random if random else DefaultSimRandom()
        self._timer = MockTimer()
        self._network = SimNetwork(self._timer, self._random, self._serialize_deserialize)
        self._nodes = []
        self._genesis_txns = None
        self._genesis_validators = genNodeNames(node_count)
        self.validators = self._genesis_validators
        self._internal_buses = {}
        self._node_votes = {}
        self._ports = self._random.sample(range(9000, 9999), 2 * len(self._genesis_validators))
        # ToDo: need to remove after implementation catchup_service (INDY-2148)
        #  and when we can change pool after NODE txn
        self._expected_node_reg = self.validators

        # Actions
        self._generate_genensis_txns()

        # Create nodes from genesis
        for name in self._genesis_validators:
            self.add_new_node(name)

    def _get_free_port(self):
        return self._ports.pop()

    def _generate_genensis_txns(self):
        self._genesis_txns = create_pool_txn_data(
            node_names=self._genesis_validators,
            crypto_factory=create_default_bls_crypto_factory(),
            get_free_port=self._get_free_port)['txns']

    def add_new_node(self, name, view_no=None):
        _view_no = view_no if view_no is not None else self._initial_view_no
        if name not in self.validators:
            self.validators.append(name)

        # TODO: emulate it the same way as in Replica, that is sender must have 'node_name:inst_id' form
        replica_name = generateName(name, 0)
        handler = partial(self.network._send_message, replica_name)
        write_manager = create_test_write_req_manager(name, self._genesis_txns)
        write_manager.node_reg_handler.committed_node_reg_at_beginning_of_view[0] = self._genesis_validators
        write_manager.node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] = self._genesis_validators
        _internal_bus = InternalBus()
        self._internal_buses[name] = _internal_bus
        self._subscribe_to_internal_msgs(name)
        replica = ReplicaService(replica_name,
                                 self.validators,
                                 _view_no,
                                 self._timer,
                                 _internal_bus,
                                 self.network.create_peer(name, handler),
                                 write_manager=write_manager,
                                 bls_bft_replica=MockBlsBftReplica())
        replica._data.node_mode = Mode.participating
        self._nodes.append(replica)
        self._update_connecteds()
        logger.info("Node {} was added into pool".format(name))

    def remove_node(self, name):
        if name not in self.validators:
            raise LogicError("Node with name {} does not exist in pool".format(name))

        self.validators.remove(name)
        replicas = set([n for n in self.nodes if n.name == name])
        assert len(replicas) == 1
        node_obj = replicas.pop()
        self._nodes.remove(node_obj)
        self._update_connecteds()
        logger.info("Node {} was removed from pool".format(name))

    def _subscribe_to_internal_msgs(self, name):
        if name not in self._internal_buses:
            raise LogicError("For node {} does not exist internal bus".format(name))
        _bus = self._internal_buses.get(name)
        _bus.subscribe(NeedAddNode, self._process_add_new_node)
        _bus.subscribe(NeedRemoveNode, self._process_remove_node)

    def _process_add_new_node(self, msg: NeedAddNode):
        self._node_votes.setdefault(msg.name, 0)
        self._node_votes[msg.name] += 1
        # ToDo: Maybe need to use real Quorum. For now it's just a prototype
        if self._node_votes[msg.name] >= self._random.integer(0, self.size) and msg.name not in self._expected_node_reg:
            # self.initiate_view_change()
            self._expected_node_reg.append(msg.name)
            # ToDo: uncomment this calls when INDY-2148 will be implemented
            # self.add_new_node(msg.name)
            # self._emulate_catchup(msg.name)
            self._node_votes.clear()

    def _process_remove_node(self, msg: NeedRemoveNode):
        self._node_votes.setdefault(msg.name, 0)
        self._node_votes[msg.name] += 1
        # ToDo: Maybe need to use real Quorum. For now it's just a prototype
        if self._node_votes[msg.name] > self.f + 1 and msg.name in self._expected_node_reg:
            # self.initiate_view_change()
            self._expected_node_reg.remove(msg.name)
            # ToDo: uncomment this call when INDY-2148 will be implemented
            # self.remove_node(msg.name)
            self._node_votes.clear()

    def sim_send_requests(self, reqs):
        faulty = (self.size - 1) // 3
        for node in self.nodes:
            for req in reqs:
                node._data.requests.add(req)
                node._data.requests.mark_as_forwarded(req, faulty + 1)
                node._data.requests.set_finalised(req)
                node.ready_for_3pc(ReqKey(req.key))

    def initiate_view_change(self, view_no=None):
        view_no = view_no if view_no else self.view_no + 1
        for _bus in self._internal_buses.values():
            _bus.send(NeedViewChange(view_no))

    # ToDo: remove it after catchup_service integration
    def _emulate_catchup(self, node_name):
        node_donor = self.nodes[0]
        node_recipient = [n for n in self.nodes if n.name.split(':')[0] == node_name][0]
        node_recipient._write_manager.database_manager = copy.copy(node_donor._write_manager.database_manager)
        node_recipient._internal_bus.send(CatchupFinished(node_donor._orderer.last_ordered_3pc,
                                                          node_donor._orderer.last_ordered_3pc))
        node_recipient._write_manager.on_catchup_finished()

    @property
    def timer(self) -> MockTimer:
        return self._timer

    @property
    def network(self) -> SimNetwork:
        return self._network

    @property
    def nodes(self) -> List[ReplicaService]:
        return self._nodes

    @property
    def size(self):
        return len(self.nodes)

    @property
    def f(self):
        return getMaxFailures(self.size)

    @property
    def view_no(self):
        vs = set([n._data.view_no for n in self.nodes])
        assert len(vs) == 1
        return vs.pop()

    def _serialize_deserialize(self, msg):
        serialized_msg = Batched().prepForSending(msg)
        serialized_msg = ZStack.serializeMsg(serialized_msg)
        new_msg = node_message_factory.get_instance(**ZStack.deserializeMsg(serialized_msg))
        # TODO: Figure out why BatchIDs are not deserialized back
        if not isinstance(msg, (MessageRep, OldViewPrePrepareRequest, OldViewPrePrepareReply)):
            assert MessageProcessor().toDict(msg) == MessageProcessor().toDict(new_msg), \
                "\n {} \n {}".format(MessageProcessor().toDict(msg), MessageProcessor().toDict(new_msg))
        return new_msg

    def _update_connecteds(self):
        connecteds = {replica_name_to_node_name(replica.name) for replica in self._nodes}
        for replica in self._nodes:
            replica._network.update_connecteds(connecteds)


VIEW_CHANGE_SERVICE_FIELDS = 'view_no', 'waiting_for_new_view', 'primary_name', 'prev_view_prepare_cert'
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
    return [Checkpoint(instId=0, viewNo=view_no, seqNoStart=0, seqNoEnd=200, digest=cp_digest(200))]


def create_pre_prepares(view_no):
    return [create_pre_prepare_no_bls(generate_state_root(), view_no=view_no, pp_seq_no=11),
            create_pre_prepare_no_bls(generate_state_root(), view_no=view_no, pp_seq_no=12),
            create_pre_prepare_no_bls(generate_state_root(), view_no=view_no, pp_seq_no=13)]


def create_batches_from_preprepares(preprepares):
    return [preprepare_to_batch_id(pp) for pp in preprepares]


def create_batches(view_no):
    return [BatchID(view_no, view_no, 11, "d1"),
            BatchID(view_no, view_no, 12, "d2"),
            BatchID(view_no, view_no, 13, "d3")]


def create_view_change(initial_view_no, stable_cp=10, batches=None):
    if batches is None:
        batches = create_batches(initial_view_no)
    digest = cp_digest(stable_cp)
    cp = Checkpoint(instId=0, viewNo=initial_view_no, seqNoStart=0, seqNoEnd=stable_cp, digest=digest)
    return ViewChange(viewNo=initial_view_no + 1,
                      stableCheckpoint=stable_cp,
                      prepared=batches,
                      preprepared=batches,
                      checkpoints=[cp])


def create_new_view_from_vc(vc, validators, checkpoint=None, batches=None):
    vc_digest = view_change_digest(vc)
    vcs = [[node_name, vc_digest] for node_name in validators]
    checkpoint = checkpoint or vc.checkpoints[0]
    batches = batches or vc.prepared
    return NewView(vc.viewNo,
                   sorted(vcs, key=itemgetter(0)),
                   checkpoint,
                   batches)


def create_new_view(initial_view_no, stable_cp, validators=None, batches=None):
    validators = validators or genNodeNames(4)
    batches = create_batches(initial_view_no) if batches is None else batches
    vc = create_view_change(initial_view_no, stable_cp, batches)
    return create_new_view_from_vc(vc, validators)


def create_view_change_acks(vc, vc_frm, senders):
    digest = view_change_digest(vc)
    senders = [name for name in senders if name != vc_frm]
    return [(ViewChangeAck(viewNo=vc.viewNo, name=vc_frm, digest=digest), ack_frm) for ack_frm in senders]


def primary_in_view(validators, view_no):
    return RoundRobinConstantNodesPrimariesSelector(validators).select_primaries(view_no=view_no)[0]

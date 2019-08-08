from typing import Optional

import base58
from plenum.common.constants import POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID
from plenum.common.event_bus import InternalBus
from plenum.common.timer import QueueTimer
from plenum.common.util import get_utc_epoch
from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector
from plenum.server.database_manager import DatabaseManager

from plenum.server.propagator import Requests

from plenum.server.node import Node

from plenum.common.metrics_collector import NullMetricsCollector
from plenum.server.view_change.node_view_changer import create_view_changer
from stp_core.types import HA

from plenum.common.startable import Mode
from plenum.common.messages.node_messages import ViewChangeDone
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.common.ledger_manager import LedgerManager
from plenum.common.config_util import getConfigOnce

whitelist = ['but majority declared']


class FakeLedger:
    def __init__(self, ledger_id, size):
        self._size = size
        self.root_hash = base58.b58encode(str(ledger_id).encode() * 32).decode("utf-8")
        self.hasher = None

    def __len__(self):
        return self._size


# Question: Why doesn't this subclass Node.
class FakeNode:
    ledger_ids = [POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID]

    def __init__(self, tmpdir, config=None):
        self.basedirpath = tmpdir
        self.name = 'Node1'
        self.internal_bus = InternalBus()
        self.db_manager = DatabaseManager()
        self.timer = QueueTimer()
        self.f = 1
        self.replicas = dict()
        self.requests = Requests()
        self.rank = None
        self.allNodeNames = [self.name, 'Node2', 'Node3', 'Node4']
        self.nodeReg = {
            name: HA("127.0.0.1", 0) for name in self.allNodeNames
        }
        self.nodeIds = []
        self.totalNodes = len(self.allNodeNames)
        self.mode = Mode.starting
        self.config = config or getConfigOnce()
        self.nodeStatusDB = None
        self.replicas = {
            0: Replica(node=self, instId=0, isMaster=True, config=self.config),
            1: Replica(node=self, instId=1, isMaster=False, config=self.config),
            2: Replica(node=self, instId=2, isMaster=False, config=self.config)
        }
        self.requiredNumberOfInstances = 2
        self._found = False
        self.ledgerManager = LedgerManager(self)
        ledger0 = FakeLedger(0, 10)
        ledger1 = FakeLedger(1, 5)
        self.ledgerManager.addLedger(0, ledger0)
        self.ledgerManager.addLedger(1, ledger1)
        self.quorums = Quorums(self.totalNodes)
        self.view_changer = create_view_changer(self)
        self.primaries_selector = RoundRobinPrimariesSelector()
        self.metrics = NullMetricsCollector()

        # For catchup testing
        self.catchup_rounds_without_txns = 0
        self.view_change_in_progress = False
        self.ledgerManager.last_caught_up_3PC = (0, 0)
        self.master_last_ordered_3PC = (0, 0)
        self.seqNoDB = {}

        # callbacks
        self.onBatchCreated = lambda self, *args, **kwargs: True

    @property
    def viewNo(self):
        return None if self.view_changer is None else self.view_changer.view_no

    @property
    def ledger_summary(self):
        return [li.ledger_summary for li in
                self.ledgerManager.ledgerRegistry.values()]

    def get_name_by_rank(self, name, node_reg, node_ids):
        # This is used only for getting name of next primary, so
        # it just returns a constant
        return 'Node2'

    def primary_selected(self, instance_id):
        self._found = True

    def is_primary_found(self):
        return self._found

    @property
    def master_primary_name(self) -> Optional[str]:
        nm = self.replicas[0].primaryName
        if nm:
            return Replica.getNodeName(nm)

    @property
    def master_replica(self):
        return self.replicas[0]

    @property
    def is_synced(self):
        return self.mode >= Mode.synced

    def on_view_change_start(self):
        pass

    def start_catchup(self):
        pass

    def allLedgersCaughtUp(self):
        Node.allLedgersCaughtUp(self)

    def _clean_non_forwarded_ordered(self):
        return Node._clean_non_forwarded_ordered(self)

    def num_txns_caught_up_in_last_catchup(self):
        return Node.num_txns_caught_up_in_last_catchup(self)

    def set_view_change_status(self, value):
        return Node.set_view_change_status(self, value)

    def mark_request_as_executed(self, request):
        Node.mark_request_as_executed(self, request)

    def _clean_req_from_verified(self, request):
        pass

    def doneProcessingReq(self, key):
        pass

    def is_catchup_needed(self):
        return False

    def no_more_catchups_needed(self):
        pass

    def select_primaries(self):
        pass

    def utc_epoch(self):
        return get_utc_epoch()

    def get_validators(self):
        return []

    def set_view_for_replicas(self, a):
        pass

    def get_primaries_for_current_view(self):
        # This is used only for getting name of next primary, so
        # it just returns a constant
        return ['Node2', 'Node3']


def test_has_view_change_quorum_number(tconf, tdir):
    """
    Checks method _hasViewChangeQuorum of SimpleSelector
    It must have n-f ViewChangeDone

    Check it for a case of view change (view_change_in_progress = True, propagate_primary = False)
    """

    ledgerInfo = (
        # ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3')
    )

    node = FakeNode(str(tdir), tconf)
    node.view_changer.view_change_in_progress = True
    node.view_changer.propagate_primary = False

    assert not node.view_changer._hasViewChangeQuorum

    # Accessing _view_change_done directly to avoid influence of methods
    node.view_changer._view_change_done = {}

    def declare(replica_name):
        node.view_changer._view_change_done[replica_name] = ('Node2', ledgerInfo)

    # Declare the Primary first and check that n-f are required
    declare('Node2')
    assert node.view_changer.has_view_change_from_primary
    assert not node.view_changer._hasViewChangeQuorum
    declare('Node1')
    assert node.view_changer.has_view_change_from_primary
    assert not node.view_changer._hasViewChangeQuorum
    declare('Node3')
    assert node.view_changer.has_view_change_from_primary
    assert node.view_changer._hasViewChangeQuorum


def test_has_view_change_quorum_must_contain_primary(tconf, tdir):
    """
    Checks method _hasViewChangeQuorum of SimpleSelector
    It must have n-f ViewChangeDone including a VCD from the next Primary

    Check it for a case of view change (view_change_in_progress = True, propagate_primary = False)
    """

    ledgerInfo = (
        # ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3')
    )

    node = FakeNode(str(tdir), tconf)
    node.view_changer.view_change_in_progress = True
    node.view_changer.propagate_primary = False

    assert not node.view_changer._hasViewChangeQuorum

    # Accessing _view_change_done directly to avoid influence of methods
    node.view_changer._view_change_done = {}

    def declare(replica_name):
        node.view_changer._view_change_done[replica_name] = ('Node2', ledgerInfo)

    declare('Node1')
    assert not node.view_changer._hasViewChangeQuorum
    assert not node.view_changer.has_view_change_from_primary
    declare('Node3')
    assert not node.view_changer._hasViewChangeQuorum
    assert not node.view_changer.has_view_change_from_primary
    declare('Node4')
    assert node.view_changer._hasViewChangeQuorum
    assert not node.view_changer.has_view_change_from_primary

    # Three nodes is enough for quorum, but there is no Node2:0 which is
    # expected to be next primary, so no quorum should be achieved
    declare('Node2')
    assert node.view_changer._hasViewChangeQuorum
    assert node.view_changer.has_view_change_from_primary


def test_process_view_change_done(tdir, tconf):
    ledgerInfo = (
        # ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3')
    )
    msg = ViewChangeDone(viewNo=0,
                         name='Node2',
                         ledgerInfo=ledgerInfo)
    node = FakeNode(str(tdir), tconf)
    quorum = node.view_changer.quorum
    for i in range(quorum):
        node.view_changer.process_vchd_msg(msg, 'Node2')
    assert node.view_changer._view_change_done
    assert not node.is_primary_found()

    node.view_changer.process_vchd_msg(msg, 'Node1')
    assert node.view_changer._view_change_done
    assert not node.is_primary_found()

    node.view_changer.process_vchd_msg(msg, 'Node3')
    assert node.view_changer._verify_primary(msg.name, msg.ledgerInfo)
    node.view_changer._start_selection()
    assert node.view_changer._view_change_done
    # Since the FakeNode does not have setting of mode
    # assert node.is_primary_found()
    node.view_changer.pre_vc_strategy = None
    node.view_changer.start_view_change(1)
    assert not node.view_changer._view_change_done


def test_get_msgs_for_lagged_nodes(tconf, tdir):
    ledgerInfo = (
        #  ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3'),
    )
    messages = [
        (ViewChangeDone(
            viewNo=0,
            name='Node2',
            ledgerInfo=ledgerInfo),
         'Node1'),
        (ViewChangeDone(
            viewNo=0,
            name='Node3',
            ledgerInfo=ledgerInfo),
         'Node2')]
    node = FakeNode(str(tdir), tconf)
    for message in messages:
        node.view_changer.process_vchd_msg(*message)

    messages_for_lagged = node.view_changer.get_msgs_for_lagged_nodes()
    assert {m for m in messages_for_lagged} == {
        m[0] for m in messages if m[1] == node.name}


def test_send_view_change_done_message(tdir, tconf):
    node = FakeNode(str(tdir), tconf)
    node.view_changer._send_view_change_done_message()

    ledgerInfo = [
        #  ledger id, ledger length, merkle root
        (0, 10, '4F7BsTMVPKFshM1MwLf6y23cid6fL3xMpazVoF9krzUw'),
        (1, 5, '4K2V1kpVycZ6qSFsNdz2FtpNxnJs17eBNzf9rdCMcKoe'),
    ]
    messages = [
        ViewChangeDone(viewNo=0, name='Node2', ledgerInfo=ledgerInfo)
    ]

    assert len(node.view_changer.outBox) == 1
    assert list(node.view_changer.outBox) == messages

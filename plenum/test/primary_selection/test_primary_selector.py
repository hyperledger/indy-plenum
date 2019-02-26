from typing import Optional
from contextlib import ExitStack

import base58
import pytest
from plenum.common.constants import POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID
from plenum.common.timer import QueueTimer
from plenum.common.util import get_utc_epoch

from plenum.server.propagator import Requests

from plenum.server.node import Node

from plenum.common.metrics_collector import NullMetricsCollector
from plenum.server.view_change.node_view_changer import create_view_changer
from stp_core.types import HA

from plenum.common.startable import Mode
from plenum.server.primary_selector import PrimarySelector
from plenum.common.messages.node_messages import ViewChangeDone
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.common.ledger_manager import LedgerManager
from plenum.common.config_util import getConfigOnce
from plenum.test.helper import create_new_test_node

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
        self.timer = QueueTimer()
        self.f = 1
        self.replicas = dict()
        self.requests = Requests()
        self.rank = None
        self.allNodeNames = [self.name, 'Node2', 'Node3', 'Node4']
        self.nodeReg = {
            name: HA("127.0.0.1", 0) for name in self.allNodeNames
        }
        self.totalNodes = len(self.allNodeNames)
        self.mode = Mode.starting
        self.config = config or getConfigOnce()
        self.nodeStatusDB = None
        self.replicas = {
            0: Replica(node=self, instId=0, isMaster=True, config=self.config),
            1: Replica(node=self, instId=1, isMaster=False, config=self.config),
            2: Replica(node=self, instId=2, isMaster=False, config=self.config),
        }
        self._found = False
        self.ledgerManager = LedgerManager(self)
        ledger0 = FakeLedger(0, 10)
        ledger1 = FakeLedger(1, 5)
        self.ledgerManager.addLedger(0, ledger0)
        self.ledgerManager.addLedger(1, ledger1)
        self.quorums = Quorums(self.totalNodes)
        self.view_changer = create_view_changer(self)
        self.elector = PrimarySelector(self)
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

    def get_name_by_rank(self, name, nodeReg=None):
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

    def mark_request_as_executed(self, request):
        Node.mark_request_as_executed(self, request)

    def _clean_req_from_verified(self, request):
        pass

    def doneProcessingReq(self, key):
        pass

    def processStashedOrderedReqs(self):
        pass

    def is_catchup_needed(self):
        return False

    def no_more_catchups_needed(self):
        pass

    def select_primaries(self):
        pass

    def utc_epoch(self):
        return get_utc_epoch()


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


def test_has_view_change_quorum_number_propagate_primary(tconf, tdir):
    """
    Checks method _hasViewChangeQuorum of SimpleSelector
    It must have f+1 ViewChangeDone in the case of PrimaryPropagation

    Check it for a case of view change (view_change_in_progress = True, propagate_primary = True)
    """

    ledgerInfo = (
        # ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3')
    )

    node = FakeNode(str(tdir), tconf)
    node.view_changer.view_change_in_progress = True
    node.view_changer.propagate_primary = True

    assert not node.view_changer._hasViewChangeQuorum

    # Accessing _view_change_done directly to avoid influence of methods
    node.view_changer._view_change_done = {}

    def declare(replica_name):
        node.view_changer._view_change_done[replica_name] = ('Node2', ledgerInfo)

    # Declare the Primary first and check that f+1 are required
    declare('Node2')
    assert node.view_changer.has_view_change_from_primary
    assert not node.view_changer._hasViewChangeQuorum
    declare('Node1')
    assert node.view_changer._hasViewChangeQuorum
    assert node.view_changer.has_view_change_from_primary


def test_has_view_change_quorum_number_must_contain_primary_propagate_primary(tconf, tdir):
    """
    Checks method _hasViewChangeQuorum of SimpleSelector
    It must have f+1 ViewChangeDone and contain a VCD from the next Primary in the case of PrimaryPropagation

    Check it for a case of view change (view_change_in_progress = True, propagate_primary = True)
    """

    ledgerInfo = (
        # ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3')
    )

    node = FakeNode(str(tdir), tconf)
    node.view_changer.view_change_in_progress = True
    node.view_changer.propagate_primary = True

    assert not node.view_changer._hasViewChangeQuorum

    # Accessing _view_change_done directly to avoid influence of methods
    node.view_changer._view_change_done = {}

    def declare(replica_name):
        node.view_changer._view_change_done[replica_name] = ('Node2', ledgerInfo)

    # Declare the Primary first and check that f+1 are required
    declare('Node1')
    assert not node.view_changer.has_view_change_from_primary
    assert not node.view_changer._hasViewChangeQuorum
    declare('Node3')
    assert not node.view_changer.has_view_change_from_primary
    assert node.view_changer._hasViewChangeQuorum

    declare('Node2')
    assert node.view_changer.has_view_change_from_primary
    assert node.view_changer._hasViewChangeQuorum


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
    node.view_changer.startViewChange(1)
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
    view_no = node.view_changer.view_no
    new_primary_name = node.elector.node.get_name_by_rank(node.elector._get_master_primary_id(
        view_no, node.totalNodes))
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

    print(list(node.view_changer.outBox))
    print(messages)

    assert list(node.view_changer.outBox) == messages


nodeCount = 7


@pytest.fixture(scope="module")
def txnPoolNodeSetWithElector(node_config_helper_class,
                              patchPluginManager,
                              tdirWithPoolTxns,
                              tdirWithDomainTxns,
                              tdir,
                              tconf,
                              poolTxnNodeNames,
                              allPluginsPath,
                              tdirWithNodeKeepInited,
                              testNodeClass,
                              do_post_node_creation):
    with ExitStack() as exitStack:
        nodes = []
        for nm in poolTxnNodeNames:
            node = exitStack.enter_context(create_new_test_node(
                testNodeClass, node_config_helper_class, nm, tconf, tdir,
                allPluginsPath))
            do_post_node_creation(node)
            nodes.append(node)
        assert len(nodes) == 7
        for node in nodes:
            node.view_changer = node.newViewChanger()
            node.elector = node.newPrimaryDecider()
        yield nodes


def test_primaries_selection_viewno_0(txnPoolNodeSetWithElector):
    view_no = 0

    for node in txnPoolNodeSetWithElector:
        assert node.replicas.num_replicas == 3
        primaries = set()
        view_no_bak = node.elector.viewNo
        node.elector.viewNo = view_no
        name, instance_name = node.elector.next_primary_replica_name_for_master(node.nodeReg)
        master_primary_rank = node.poolManager.get_rank_by_name(name)
        assert master_primary_rank == 0
        assert name == "Alpha" and instance_name == "Alpha:0"
        primaries.add(name)

        instance_id = 1
        name, instance_name = node.elector.next_primary_replica_name_for_backup(
            instance_id, master_primary_rank, primaries)
        primary_rank = node.poolManager.get_rank_by_name(name)
        assert primary_rank == 1
        assert name == "Beta" and instance_name == "Beta:1"
        primaries.add(name)

        instance_id = 2
        name, instance_name = node.elector.next_primary_replica_name_for_backup(
            instance_id, master_primary_rank, primaries)
        primary_rank = node.poolManager.get_rank_by_name(name)
        assert primary_rank == 2
        assert name == "Gamma" and instance_name == "Gamma:2"
        primaries.add(name)

        node.elector.viewNo = view_no_bak


def test_primaries_selection_viewno_5(txnPoolNodeSetWithElector):
    view_no = 5

    for node in txnPoolNodeSetWithElector:
        assert node.replicas.num_replicas == 3
        primaries = set()
        view_no_bak = node.elector.viewNo
        node.elector.viewNo = view_no
        name, instance_name = node.elector.next_primary_replica_name_for_master(node.nodeReg)
        master_primary_rank = node.poolManager.get_rank_by_name(name)
        assert master_primary_rank == 5
        assert name == "Zeta" and instance_name == "Zeta:0"
        primaries.add(name)

        instance_id = 1
        name, instance_name = node.elector.next_primary_replica_name_for_backup(
            instance_id, master_primary_rank, primaries)
        primary_rank = node.poolManager.get_rank_by_name(name)
        assert primary_rank == 6
        assert name == "Eta" and instance_name == "Eta:1"
        primaries.add(name)

        instance_id = 2
        name, instance_name = node.elector.next_primary_replica_name_for_backup(
            instance_id, master_primary_rank, primaries)
        primary_rank = node.poolManager.get_rank_by_name(name)
        assert primary_rank == 0
        assert name == "Alpha" and instance_name == "Alpha:2"
        primaries.add(name)

        node.elector.viewNo = view_no_bak


def test_primaries_selection_viewno_9(txnPoolNodeSetWithElector):
    view_no = 9

    for node in txnPoolNodeSetWithElector:
        assert node.replicas.num_replicas == 3
        primaries = set()
        view_no_bak = node.elector.viewNo
        node.elector.viewNo = view_no
        name, instance_name = node.elector.next_primary_replica_name_for_master(node.nodeReg)
        master_primary_rank = node.poolManager.get_rank_by_name(name)
        assert master_primary_rank == 2
        assert name == "Gamma" and instance_name == "Gamma:0"
        primaries.add(name)

        instance_id = 1
        name, instance_name = node.elector.next_primary_replica_name_for_backup(
            instance_id, master_primary_rank, primaries)
        primary_rank = node.poolManager.get_rank_by_name(name)
        assert primary_rank == 3
        assert name == "Delta" and instance_name == "Delta:1"
        primaries.add(name)

        instance_id = 2
        name, instance_name = node.elector.next_primary_replica_name_for_backup(
            instance_id, master_primary_rank, primaries)
        primary_rank = node.poolManager.get_rank_by_name(name)
        assert primary_rank == 4
        assert name == "Epsilon" and instance_name == "Epsilon:2"
        primaries.add(name)

        node.elector.viewNo = view_no_bak


def test_primaries_selection_gaps(txnPoolNodeSetWithElector):
    for node in txnPoolNodeSetWithElector:
        assert node.replicas.num_replicas == 3
        """
        The gaps between primary nodes may occur due to nodes demotion and promotion
        with changed number of instances. These gaps should be filled by new
        primaries during primary selection for new backup instances created as
        a result of instances growing.
        """
        primary_0 = "Beta"
        primary_1 = "Delta"
        primaries = {primary_0, primary_1}
        master_primary_rank = node.poolManager.get_rank_by_name(primary_0)
        name, instance_name = node.elector.next_primary_replica_name_for_backup(
            2, master_primary_rank, primaries)
        assert name == "Gamma" and \
               instance_name == "Gamma:2"

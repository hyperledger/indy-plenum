from typing import Optional

import base58
import pytest

from stp_core.types import HA

from plenum.common.startable import Mode
from plenum.server.primary_selector import PrimarySelector
from plenum.common.messages.node_messages import ViewChangeDone
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.common.ledger_manager import LedgerManager


whitelist = ['but majority declared']


class FakeLedger:
    def __init__(self, ledger_id, size):
        self._size = size
        self.root_hash = base58.b58encode(str(ledger_id).encode() * 32)
        self.hasher = None

    def __len__(self):
        return self._size


# Question: Why doesn't this subclass Node.
class FakeNode:
    ledger_ids = [0]

    def __init__(self, tmpdir):
        self.basedirpath = tmpdir
        self.name = 'Node1'
        self.f = 1
        self.replicas = []
        self.viewNo = 0
        self.rank = None
        self.allNodeNames = [self.name, 'Node2', 'Node3', 'Node4']
        self.nodeReg = {
            name: HA("127.0.0.1", 0) for name in self.allNodeNames
        }
        self.totalNodes = len(self.allNodeNames)
        self.mode = Mode.starting
        self.replicas = [
            Replica(node=self, instId=0, isMaster=True),
            Replica(node=self, instId=1, isMaster=False),
            Replica(node=self, instId=2, isMaster=False),
        ]
        self._found = False
        self.ledgerManager = LedgerManager(self, ownedByNode=True)
        ledger0 = FakeLedger(0, 10)
        ledger1 = FakeLedger(1, 5)
        self.ledgerManager.addLedger(0, ledger0)
        self.ledgerManager.addLedger(1, ledger1)
        self.quorums = Quorums(self.totalNodes)
        self.view_change_in_progress = True
        self.propagate_primary = False

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


def test_has_view_change_quorum_number(tmpdir):
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

    node = FakeNode(str(tmpdir))
    node.view_change_in_progress = True
    node.propagate_primary = False
    selector = PrimarySelector(node)

    assert not selector._hasViewChangeQuorum

    # Accessing _view_change_done directly to avoid influence of methods
    selector._view_change_done = {}

    def declare(replica_name):
        selector._view_change_done[replica_name] = ('Node2', ledgerInfo)

    # Declare the Primary first and check that n-f are required
    declare('Node2')
    assert selector.has_view_change_from_primary
    assert not selector._hasViewChangeQuorum
    declare('Node1')
    assert selector.has_view_change_from_primary
    assert not selector._hasViewChangeQuorum
    declare('Node3')
    assert selector.has_view_change_from_primary
    assert selector._hasViewChangeQuorum


def test_has_view_change_quorum_must_contain_primary(tmpdir):
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

    node = FakeNode(str(tmpdir))
    node.view_change_in_progress = True
    node.propagate_primary = False
    selector = PrimarySelector(node)

    assert not selector._hasViewChangeQuorum

    # Accessing _view_change_done directly to avoid influence of methods
    selector._view_change_done = {}

    def declare(replica_name):
        selector._view_change_done[replica_name] = ('Node2', ledgerInfo)

    declare('Node1')
    assert not selector._hasViewChangeQuorum
    assert not selector.has_view_change_from_primary
    declare('Node3')
    assert not selector._hasViewChangeQuorum
    assert not selector.has_view_change_from_primary
    declare('Node4')
    assert selector._hasViewChangeQuorum
    assert not selector.has_view_change_from_primary

    # Three nodes is enough for quorum, but there is no Node2:0 which is
    # expected to be next primary, so no quorum should be achieved
    declare('Node2')
    assert selector._hasViewChangeQuorum
    assert selector.has_view_change_from_primary


def test_has_view_change_quorum_number_propagate_primary(tmpdir):
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

    node = FakeNode(str(tmpdir))
    node.view_change_in_progress = True
    node.propagate_primary = True
    selector = PrimarySelector(node)

    assert not selector._hasViewChangeQuorum

    # Accessing _view_change_done directly to avoid influence of methods
    selector._view_change_done = {}

    def declare(replica_name):
        selector._view_change_done[replica_name] = ('Node2', ledgerInfo)

    # Declare the Primary first and check that f+1 are required
    declare('Node2')
    assert selector.has_view_change_from_primary
    assert not selector._hasViewChangeQuorum
    declare('Node1')
    assert selector._hasViewChangeQuorum
    assert selector.has_view_change_from_primary


def test_has_view_change_quorum_number_must_contain_primary_propagate_primary(tmpdir):
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

    node = FakeNode(str(tmpdir))
    node.view_change_in_progress = True
    node.propagate_primary = True
    selector = PrimarySelector(node)

    assert not selector._hasViewChangeQuorum

    # Accessing _view_change_done directly to avoid influence of methods
    selector._view_change_done = {}

    def declare(replica_name):
        selector._view_change_done[replica_name] = ('Node2', ledgerInfo)

    # Declare the Primary first and check that f+1 are required
    declare('Node1')
    assert not selector.has_view_change_from_primary
    assert not selector._hasViewChangeQuorum
    declare('Node3')
    assert not selector.has_view_change_from_primary
    assert selector._hasViewChangeQuorum

    declare('Node2')
    assert selector.has_view_change_from_primary
    assert selector._hasViewChangeQuorum


def test_process_view_change_done(tmpdir):
    ledgerInfo = (
        # ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3')
    )
    msg = ViewChangeDone(viewNo=0,
                         name='Node2',
                         ledgerInfo=ledgerInfo)
    node = FakeNode(str(tmpdir))
    selector = PrimarySelector(node)
    quorum = selector.quorum
    for i in range(quorum):
        selector._processViewChangeDoneMessage(msg, 'Node2')
    assert selector._view_change_done
    assert not node.is_primary_found()

    selector._processViewChangeDoneMessage(msg, 'Node1')
    assert selector._view_change_done
    assert not node.is_primary_found()

    selector._processViewChangeDoneMessage(msg, 'Node3')
    assert selector._verify_primary(msg.name, msg.ledgerInfo)
    selector._start_selection()
    assert selector._view_change_done
    # Since the FakeNode does not have setting of mode
    # assert node.is_primary_found()
    selector.view_change_started(1)
    assert not selector._view_change_done


def test_get_msgs_for_lagged_nodes(tmpdir):
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
    node = FakeNode(str(tmpdir))
    selector = PrimarySelector(node)
    for message in messages:
        selector._processViewChangeDoneMessage(*message)

    messages_for_lagged = selector.get_msgs_for_lagged_nodes()
    assert {m for m in messages_for_lagged} == {
        m[0] for m in messages if m[1] == node.name}


def test_send_view_change_done_message(tmpdir):
    node = FakeNode(str(tmpdir))
    selector = PrimarySelector(node)
    instance_id = 0
    view_no = selector.viewNo
    new_primary_name = selector.node.get_name_by_rank(selector._get_primary_id(
        view_no, instance_id, node.totalNodes))
    selector._send_view_change_done_message()

    ledgerInfo = [
        #  ledger id, ledger length, merkle root
        (0, 10, '4F7BsTMVPKFshM1MwLf6y23cid6fL3xMpazVoF9krzUw'),
        (1, 5, '4K2V1kpVycZ6qSFsNdz2FtpNxnJs17eBNzf9rdCMcKoe'),
    ]
    messages = [
        ViewChangeDone(viewNo=0, name='Node2', ledgerInfo=ledgerInfo)
    ]

    assert len(selector.outBox) == 1

    print(list(selector.outBox))
    print(messages)

    assert list(selector.outBox) == messages

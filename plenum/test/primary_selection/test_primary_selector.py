from typing import Optional

import base58
import pytest

from stp_core.types import HA

from plenum.common.startable import Mode
from plenum.server.primary_selector import PrimarySelector
from plenum.server.view_change.view_changer import ViewChanger
from plenum.common.messages.node_messages import ViewChangeDone
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.common.ledger_manager import LedgerManager
from plenum.common.config_util import getConfig


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
        self.config = getConfig() # TODO do we need fake object here?
        self.view_changer = ViewChanger(self)
        self.elector = PrimarySelector(self)

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
    node.view_changer.startViewChange(1)
    assert not node.view_changer._view_change_done


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
    for message in messages:
        node.view_changer.process_vchd_msg(*message)

    messages_for_lagged = node.view_changer.get_msgs_for_lagged_nodes()
    assert {m for m in messages_for_lagged} == {
        m[0] for m in messages if m[1] == node.name}


def test_send_view_change_done_message(tmpdir):
    node = FakeNode(str(tmpdir))
    instance_id = 0
    view_no = node.view_changer.view_no
    new_primary_name = node.elector.node.get_name_by_rank(node.elector._get_primary_id(
        view_no, instance_id, node.totalNodes))
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

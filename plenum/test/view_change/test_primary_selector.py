from typing import Optional

import base58
import pytest
from plenum.server.primary_selector import PrimarySelector
from plenum.common.types import ViewChangeDone
from plenum.server.replica import Replica
from plenum.common.util import get_strong_quorum
from plenum.common.ledger_manager import LedgerManager
from plenum.common.ledger_manager import Ledger


whitelist = ['but majority declared']


class FakeLedger():
    def __init__(self, ledger_id, size):
        self._size = size
        self.root_hash = (base58.alphabet[ledger_id] * 43)
        self.hasher = None

    def __len__(self):
        return self._size


# Question: Why doesn't this subclass Node.
class FakeNode():
    def __init__(self):
        self.name = 'Node1'
        self.f = 1
        self.replicas = []
        self.viewNo = 0
        self.rank = None
        self.allNodeNames = [self.name, 'Node2', 'Node3', 'Node4']
        self.totalNodes = len(self.allNodeNames)
        self.ledger_ids = [0]
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

    def get_name_by_rank(self, name):
        # This is used only for getting name of next primary, so
        # it just returns a constant
        return 'Node2'

    def primary_found(self):
        self._found = True

    def is_primary_found(self):
        return self._found

    @property
    def master_primary_name(self) -> Optional[str]:
        nm = self.replicas[0].primaryName
        if nm:
            return Replica.getNodeName(nm)


def testHasViewChangeQuorum():
    """
    Checks method _hasViewChangeQuorum of SimpleSelector 
    """

    instance_id = 0
    last_ordered_seq_no = 0
    selector = PrimarySelector(FakeNode())

    assert not selector._hasViewChangeQuorum(instance_id)

    # Accessing _view_change_done directly to avoid influence of methods
    selector._view_change_done[instance_id] = {}

    def declare(replica_name):
        selector._view_change_done[instance_id][replica_name] = \
                {'Node2:0': last_ordered_seq_no}

    declare('Node1:0')
    declare('Node3:0')
    declare('Node4:0')

    # Three nodes is enough for quorum, but there is no Node2:0 which is
    # expected to be next primary, so no quorum should be achieved
    assert not selector._hasViewChangeQuorum(instance_id)

    declare('Node2:0')
    assert selector._hasViewChangeQuorum(instance_id)


def testProcessViewChangeDone():
    ledgerInfo = (
        # ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3')
    )
    msg = ViewChangeDone(name='Node2:0',
                         instId=0,
                         viewNo=0,
                         ledgerInfo=ledgerInfo)
    node = FakeNode()
    selector = PrimarySelector(node)
    quorum = get_strong_quorum(node.totalNodes)
    for i in range(quorum):
        selector._processViewChangeDoneMessage(msg, 'Node2')
    assert selector._view_change_done[0]
    assert not node.is_primary_found()

    selector._processViewChangeDoneMessage(msg, 'Node1')
    assert selector._view_change_done[0]
    assert not node.is_primary_found()

    selector._processViewChangeDoneMessage(msg, 'Node3')
    assert selector._verify_primary_selection(msg.instId, msg.name,
                                              msg.ledgerInfo)
    selector._startSelection()
    assert selector._view_change_done[0]
    assert node.is_primary_found()
    selector.view_change_started(1)
    assert not selector._view_change_done[0]


def test_get_msgs_for_lagged_nodes():
    ledgerInfo = (
        #  ledger id, ledger length, merkle root
        (0, 10, '7toTJZHzaxQ7cGZv18MR4PMBfuUecdEQ1JRqJVeJBvmd'),
        (1, 5, 'Hs9n4M3CrmrkWGVviGq48vSbMpCrk6WgSBZ7sZAWbJy3'),
    )
    messages = [
        (ViewChangeDone(name='Node2:0', instId=0, viewNo=0, ledgerInfo=ledgerInfo), 'Node1'),
        (ViewChangeDone(name='Node3:0', instId=0, viewNo=0, ledgerInfo=ledgerInfo), 'Node2'),
        (ViewChangeDone(name='Node2:0', instId=1, viewNo=0, ledgerInfo=ledgerInfo), 'Node3'),
        (ViewChangeDone(name='Node2:0', instId=1, viewNo=0, ledgerInfo=ledgerInfo), 'Node1'),
    ]
    node = FakeNode()
    selector = PrimarySelector(node)
    for message in messages:
        selector._processViewChangeDoneMessage(*message)

    messages_for_lagged = selector.get_msgs_for_lagged_nodes()
    assert {m for m in messages_for_lagged} == {m[0] for m in messages if m[1] == node.name}


def test_send_view_change_done_message():
    node = FakeNode()
    selector = PrimarySelector(node)
    instance_id = 0
    new_primary_name = selector._who_is_the_next_primary(instance_id)
    view_no = selector.viewNo
    selector._send_view_change_done_message(instance_id, new_primary_name,
                                            view_no)

    ledgerInfo = [
        #  ledger id, ledger length, merkle root
        (0, 10, (base58.alphabet[0] * 43)),
        (1, 5, (base58.alphabet[1] * 43)),
    ]
    messages = [
        ViewChangeDone(name='Node2:0', instId=0, viewNo=0, ledgerInfo=ledgerInfo)
    ]

    assert len(selector.outBox) == 1

    print(list(selector.outBox))
    print(messages)

    assert list(selector.outBox) == messages

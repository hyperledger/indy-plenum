import pytest
from plenum.server.primary_selector import PrimarySelector
from plenum.common.types import ViewChangeDone
from plenum.server.replica import Replica


class FakeNode():
    def __init__(self):
        self.name = "Node1"
        self.f = 1
        self.replicas = []
        self.viewNo = 0
        self.rank = None
        self.allNodeNames = [self.name, "Node2", "Node3", "Node4"]
        self.totalNodes = len(self.allNodeNames)
        self.ledger_ids = [0]
        self.replicas = [
            Replica(node=self, instId=0, isMaster=True),
            Replica(node=self, instId=1, isMaster=False),
            Replica(node=self, instId=2, isMaster=False),
        ]
        self._found = False

    def get_name_by_rank(self, name):
        # This is used only for getting name of next primary, so
        # it just returns a constant
        return "Node2"

    def primary_found(self):
        self._found = True

    def is_primary_found(self):
        return self._found


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
                {"Node2:0", last_ordered_seq_no}

    declare('Node1:0')
    declare('Node3:0')
    declare('Node4:0')

    # Three nodes is enough for quorum, but there is no Node2:0 which is
    # expected to be next primary, so no quorum should be achieved
    assert not selector._hasViewChangeQuorum(instance_id)

    declare('Node2:0')
    assert selector._hasViewChangeQuorum(instance_id)


def testProcessViewChangeDone():
    msg = ViewChangeDone(name="Node2",
                         instId=0,
                         viewNo=0,
                         ordSeqNo=0)

    node = FakeNode()
    selector = PrimarySelector(node)

    from plenum.common.util import get_strong_quorum

    quorum = get_strong_quorum(node.totalNodes)
    for i in range(quorum):
        selector._processViewChangeDone(msg, "Node2")
    assert not node.is_primary_found()

    selector._processViewChangeDone(msg, "Node1")
    assert not node.is_primary_found()

    selector._processViewChangeDone(msg, "Node3")
    assert node.is_primary_found()

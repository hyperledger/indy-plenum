import pytest
from plenum.server.primary_selector import PrimarySelector
from plenum.common.types import ViewChangeDone
from plenum.server.node import Node


class FakeNode():
    def __init__(self):
        self.name = "Node1"
        self.f = 1
        self.replicas = []
        self.viewNo = 0
        self.rank = None
        self.allNodeNames = [self.name, "Node2", "Node3", "Node4"]
        self.totalNodes = len(self.allNodeNames)

    def get_name_by_rank(self, name):
        # This is used only for getting name of next primary, so
        # it just returns a constant
        return "Node2"


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
    try:
        sender = "Node2"
        msg = ViewChangeDone(name="Node1",
                             instId=0,
                             viewNo=0,
                             ordSeqNo=0)

        decider = PrimarySelector(FakeNode())
        decider.processViewChangeDone(msg, sender)
    except Exception:
        pass

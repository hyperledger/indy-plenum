import pytest
from plenum.server.primary_selector import PrimarySelector
from plenum.common.types import ViewChangeDone


class FakeNode():
    def __init__(self):
        self.name = "Node1"
        self.f = 1
        self.replicas = []
        self.viewNo = 0
        self.rank = None
        self.allNodeNames = [self.name, "Node2"]


def testHasViewChangeQuorum():
    decider = PrimarySelector(FakeNode())
    print(decider.name)


def testProcessViewChangeDone():
    sender = "Node2"
    msg = ViewChangeDone(name="Node1",
                         instId=0,
                         viewNo=0,
                         ordSeqNo=0)

    decider = PrimarySelector(FakeNode())
    decider.processViewChangeDone(msg, sender)

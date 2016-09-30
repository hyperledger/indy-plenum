import pytest
import types
from plenum.common.types import InstanceChange

def test_instance_change_msg_type_checking(nodeSet, looper, up):
    nodeA = nodeSet.Alpha
    nodeB = nodeSet.Beta
    
    ridBetta = nodeA.nodestack.getRemote(nodeB.name).uid
    badViewNo = "BAD"
    nodeA.send(InstanceChange(badViewNo), ridBetta)
    looper.runFor(.2)
import pytest

from plenum.common.constants import TXN_TYPE, NYM, NODE
from plenum.common.request import Request
from plenum.server.stateful import TransitionError
from plenum.server.rbftrequest import (
        RBFTRequest,
        RBFTReqState,
        TPCRequest,
        TPCReqState
)
from plenum.server.quorums import Quorum

@pytest.fixture()
def operation1():
    return {
        TXN_TYPE: NYM
    }

@pytest.fixture()
def operation2():
    return {
        TXN_TYPE: NODE
    }

@pytest.fixture
def masterInstId():
    return 0

@pytest.fixture
def backupInstId(masterInstId):
    return masterInstId + 1

@pytest.fixture
def rbft_request(operation1, masterInstId):
    return RBFTRequest(
        Request('123', 123, operation1),
        'nodeName',
        'clientName',
        masterInstId=masterInstId
    )

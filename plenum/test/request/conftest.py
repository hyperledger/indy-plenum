import pytest

from plenum.common.constants import TXN_TYPE, NYM, NODE
from plenum.common.request import Request
from plenum.server.stateful import TransitionError
from plenum.server.tpcrequest import (
        TPCRequest,
        TPCReqState
)
from plenum.server.rbftrequest import (
        RBFTRequest,
        RBFTReqState,
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
def master_inst_id():
    return 0

@pytest.fixture
def backup_inst_id(master_inst_id):
    return master_inst_id + 1

@pytest.fixture
def rbft_request(operation1, master_inst_id):
    return RBFTRequest(
        Request('123', 123, operation1),
        'nodeName',
        'clientName',
        master_inst_id=master_inst_id
    )

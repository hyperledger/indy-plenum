import time

import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, OP_FIELD_NAME, \
    PROPAGATE
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.messages.node_messages import Propagate
from plenum.common.request import Request
from plenum.common.signer_did import DidSigner
from plenum.common.types import OPERATION, f

CLIENT_PUBLIC_KEY = b'M2Qw^f:=4VU*+xmMF^Tk4N&Y0z)5<5SPt.{h}SE{'


original_validate = ClientMessageValidator.validate


@pytest.fixture(scope="module")
def validation_patched():
    """
    Patches request schema validation
    """
    ClientMessageValidator.validate = patched_validate
    yield
    ClientMessageValidator.validate = original_validate


@pytest.fixture(scope="function")
def req(poolTxnClientData):
    _, seed = poolTxnClientData
    signer = DidSigner(seed=seed)

    request = {
        f.IDENTIFIER.nm: signer.identifier,
        f.REQ_ID.nm: int(time.time() * 10 ** 6),
        OPERATION: {
            'amount': 62,
            'type': 'buy'
        },
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }

    request[f.SIG.nm] = signer.sign(request)

    return request


@pytest.fixture(scope="function")
def propagate(req):
    ppg = {
        OP_FIELD_NAME: PROPAGATE,
        f.REQUEST.nm: req,
        f.SENDER_CLIENT.nm: CLIENT_PUBLIC_KEY.decode()
    }

    return ppg


validation_times = 0


def patched_validate(self, dct):
    global validation_times
    validation_times += 1
    original_validate(self, dct)


def test_schema_validation_for_request_from_client(validation_patched,
                                                   txnPoolNodeSet, req):
    node = txnPoolNodeSet[0]

    validation_times_before = validation_times
    node.handleOneClientMsg((req, CLIENT_PUBLIC_KEY))
    msg = node.clientInBox.pop()  # pop the last added message
    assert isinstance(msg, Request)
    assert msg.frm == CLIENT_PUBLIC_KEY
    # TODO INDY-1983
    node.processRequest(msg)
    validation_times_after = validation_times

    # The request schema must be validated 2 times: the first time when
    # a SafeRequest instance is constructed for the incoming request from
    # a client and the second time when a Propagate instance is constructed
    # for propagating the request
    assert validation_times_after - validation_times_before == 2


def test_request_schema_validation_for_propagate(validation_patched,
                                                 txnPoolNodeSet, propagate):
    node = txnPoolNodeSet[0]
    other_node = txnPoolNodeSet[1]

    validation_times_before = validation_times
    node.handleOneNodeMsg(ZStackMessage(propagate, other_node.name))
    m = node.nodeInBox.pop()  # pop the last added message
    assert isinstance(m, Propagate)
    assert m.frm == other_node.name
    node.processPropagate(m)
    validation_times_after = validation_times

    # The request schema must be validated 2 times: the first time when
    # a Propagate instance is constructed for the incoming request propagated
    # by another node and the second time when a Propagate instance is
    # constructed for propagating the request
    assert validation_times_after - validation_times_before == 2

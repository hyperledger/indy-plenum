import pytest
from plenum.test.conftest import getValueFromModule

from plenum.client.client import Client
from plenum.test.helper import sdk_json_to_request_object
from plenum.test.malicious_behaviors_client import makeClientFaulty


@pytest.fixture("module")
def fClient(request, client1: Client):
    makeClientFaulty(client1, getValueFromModule(request, "clientFault"))
    return client1


@pytest.fixture("module")
def passThroughReqAcked1(sent1):
    # Overriding reqAcked1 in conftest.py to do nothing because the client
    # shouldn't see ReqAck msgs from all the nodes, since it only sent REQUESTs
    # to some.
    request = sdk_json_to_request_object(sent1[0][0])
    return request

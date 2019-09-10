import pytest

from plenum.server.consensus.message_request.message_req_3pc_service import MessageReq3pcService


@pytest.fixture
def data(consensus_data):
    return consensus_data("MessageReq3pcService")


@pytest.fixture
def message_req_3pc_service(data, internal_bus, external_bus):
    req_service = MessageReq3pcService(data=data,
                                       bus=internal_bus,
                                       network=external_bus)
    return req_service

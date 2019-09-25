import pytest

from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.test.helper import create_pre_prepare_no_bls, create_commit_no_bls_sig, create_prepare, generate_state_root


@pytest.fixture
def data(consensus_data):
    return consensus_data("MessageReq3pcService")


@pytest.fixture
def message_req_service(data, internal_bus, external_bus):
    req_service = MessageReqService(data=data,
                                    bus=internal_bus,
                                    network=external_bus)
    return req_service


@pytest.fixture
def pp(data):
    pp = create_pre_prepare_no_bls(generate_state_root(),
                                   pp_seq_no=1,
                                   view_no=data.view_no)
    data.sent_preprepares[(pp.viewNo, pp.ppSeqNo)] = pp
    return pp


@pytest.fixture
def commit(data):
    key = (data.view_no, 1)
    commit = create_commit_no_bls_sig(key,
                                      data.inst_id)
    data.commits.addVote(commit, data.name)
    return commit

@pytest.fixture
def prepare(data):
    key = (data.view_no, 1)
    prepare = create_prepare(key,
                             generate_state_root(),
                             data.inst_id)
    data.prepares.addVote(prepare, data.name)
    return prepare

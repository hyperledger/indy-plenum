import pytest

from plenum.common.exceptions import SuspiciousNode
from plenum.common.util import updateNamedTuple
from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.server.models import ThreePhaseVotes
from plenum.server.suspicion_codes import Suspicions
from plenum.test.consensus.order_service.helper import _register_pp_ts
from plenum.test.helper import generate_state_root, create_prepare_from_pre_prepare, create_commit_from_pre_prepare

PRIMARY_NAME = "Alpha:0"
NON_PRIMARY_NAME = "Beta:0"


@pytest.fixture(scope="function")
def prepare(_pre_prepare):
    return create_prepare_from_pre_prepare(_pre_prepare)


@pytest.fixture(scope="function")
def commit(_pre_prepare):
    return create_commit_from_pre_prepare(_pre_prepare)


@pytest.fixture(scope='function', params=['Primary', 'Non-Primary'])
def o(orderer_with_requests, request):
    assert orderer_with_requests.primary_name == PRIMARY_NAME
    if request == 'Primary':
        orderer_with_requests.name = PRIMARY_NAME
    else:
        orderer_with_requests.name = NON_PRIMARY_NAME
    return orderer_with_requests


@pytest.fixture(scope="function")
def pre_prepare(o, _pre_prepare):
    _register_pp_ts(o, _pre_prepare, o.primary_name)
    return _pre_prepare


def test_process_ordered_commit(o, pre_prepare, prepare, commit):
    o._validator = OrderingServiceMsgValidator(o._data)
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    o._data.prev_view_prepare_cert = 1
    o.last_ordered_3pc = (commit.viewNo, commit.ppSeqNo + 1)
    for i in range(o._data.quorums.prepare.value):
        if o._data.validators[i] + ":0" != o.name:
            o.process_prepare(prepare, o._data.validators[i])

    assert o.commits[(commit.viewNo, commit.ppSeqNo)] == ThreePhaseVotes(voters={o.name},
                                                                         msg=commit)
    sender = o._data.validators[-1]
    o.process_commit(commit, sender)
    assert o.commits[(commit.viewNo, commit.ppSeqNo)] == ThreePhaseVotes(voters={o.name, sender},
                                                                         msg=commit)


def test_not_order_already_ordered(o, pre_prepare, prepare, commit):
    o._validator = OrderingServiceMsgValidator(o._data)
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    o.last_ordered_3pc = (commit.viewNo, commit.ppSeqNo + 1)
    for i in range(o._data.quorums.prepare.value):
        if o._data.validators[i] + ":0" != o.name:
            o.process_prepare(prepare, o._data.validators[i])

    assert not o.ordered
    for i in range(o._data.quorums.n):
        o.process_commit(commit, o._data.validators[i])
    assert not o.ordered

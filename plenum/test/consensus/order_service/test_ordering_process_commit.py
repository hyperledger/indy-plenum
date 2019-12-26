from unittest.mock import Mock

import pytest

from plenum.server.models import ThreePhaseVotes
from plenum.test.consensus.order_service.helper import _register_pp_ts
from plenum.test.helper import create_prepare_from_pre_prepare, create_commit_from_pre_prepare

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
    if request.param == 'Primary':
        orderer_with_requests.name = PRIMARY_NAME
    else:
        orderer_with_requests.name = NON_PRIMARY_NAME
    return orderer_with_requests


@pytest.fixture(scope="function")
def pre_prepare(o, _pre_prepare):
    _register_pp_ts(o, _pre_prepare, o.primary_name)
    return _pre_prepare


def test_process_commit(o, pre_prepare, prepare, commit):
    # mock BLS validation
    validate_commit_handler = Mock(return_value=None)
    o.l_bls_bft_replica.validate_commit = validate_commit_handler

    # process PrePrepare and a quorum of Prepares
    # to be able to process Commit
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    for i in range(o._data.quorums.prepare.value):
        o.process_prepare(prepare, o._data.validators[-i])

    # make sure that Commit from this Node is processed and sent
    assert o.commits[(commit.viewNo, commit.ppSeqNo)] == ThreePhaseVotes(voters={o.name},
                                                                         msg=commit)

    # receive Commit from another node
    sender = o._data.validators[-1]
    o.process_commit(commit, sender)

    # make sure it's processed
    assert o.commits[(commit.viewNo, commit.ppSeqNo)] == ThreePhaseVotes(voters={o.name, sender},
                                                                         msg=commit)
    # make sure that BLS validation is called
    validate_commit_handler.assert_called_once_with(commit, sender, pre_prepare)


def test_process_ordered_commit(o, pre_prepare, prepare, commit):
    # mock BLS validation
    validate_commit_handler = Mock(return_value=None)
    o.l_bls_bft_replica.validate_commit = validate_commit_handler

    # make sure that Commit is already ordered
    # make sure that Commit's ppSeqNo is less than prev_view_prepare_cert, so that this is re-ordering after the view change,
    # otherwise ordered Commit would be discarded
    o._data.prev_view_prepare_cert = 10
    o.last_ordered_3pc = (commit.viewNo, commit.ppSeqNo + 1)

    # process PrePrepare and a quorum of Prepares
    # to be able to process Commit
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    for i in range(o._data.quorums.prepare.value):
        o.process_prepare(prepare, o._data.validators[-i])

    # make sure that Commit from this Node is processed and sent
    assert o.commits[(commit.viewNo, commit.ppSeqNo)] == ThreePhaseVotes(voters={o.name},
                                                                         msg=commit)

    # receive Commit from another node
    sender = o._data.validators[-1]
    o.process_commit(commit, sender)

    # make sure it's processed
    assert o.commits[(commit.viewNo, commit.ppSeqNo)] == ThreePhaseVotes(voters={o.name, sender},
                                                                         msg=commit)

    # make sure that BLS validation is not called for already Ordered
    validate_commit_handler.assert_not_called()


def test_not_order_already_ordered(o, pre_prepare, prepare, commit):
    # make sure that Commit is already ordered
    # make sure that Commit's ppSeqNo is less than prev_view_prepare_cert, so that this is re-ordering after the view change,
    # otherwise ordered Commit would be discarded
    o.last_ordered_3pc = (commit.viewNo, commit.ppSeqNo + 1)
    o._data.prev_view_prepare_cert = 10

    # process PrePrepare and a quorum of Prepares
    # to be able to process Commit
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    for i in range(o._data.quorums.prepare.value):
        if o._data.validators[i] + ":0" != o.name:
            o.process_prepare(prepare, o._data.validators[i])

    # process a quorum of ordered Commits
    assert not o.ordered
    for i in range(o._data.quorums.n):
        o.process_commit(commit, o._data.validators[i])

    # check that ordered state is not changed
    assert not o.ordered

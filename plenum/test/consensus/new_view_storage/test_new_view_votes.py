import pytest

from plenum.common.messages.node_messages import NewView
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.quorums import Quorums
from plenum.test.consensus.helper import primary_in_view


@pytest.fixture()
def rs(replica_service):
    replica_service._data.primary_name = primary_in_view(replica_service._data.validators,
                                                         replica_service._data.view_no)
    return replica_service

@pytest.fixture()
def nvm(rs):
    return NewView(viewNo=0,
                   viewChanges=[],
                   checkpoint=rs._data.initial_checkpoint,
                   batches=[])


@pytest.fixture(params=['eq', 'gt'])
def lt_eq_gt(request, rs, nvm):
    without_primaries = [v for v in rs._data.validators if v != replica_name_to_node_name(rs._data.primary_name)]
    quorums = Quorums(len(rs._data.validators))
    if request.param == 'lt':
        for i in range(0, quorums.strong.value - 1):
            rs._data.new_view_votes.add_new_view(nvm, without_primaries.pop())
        return None, rs
    if request.param == 'eq':
        for i in range(0, quorums.strong.value):
            rs._data.new_view_votes.add_new_view(nvm, without_primaries.pop())
        return nvm, rs

    for i in range(0, quorums.strong.value + 1):
        if without_primaries:
            rs._data.new_view_votes.add_new_view(nvm, without_primaries.pop())
    return nvm, rs


def test_new_view_from_primary(rs, nvm):
    rs._data.primary_name = primary_in_view(rs._data.validators,
                                                         rs._data.view_no)
    rs._data.new_view_votes.add_new_view(nvm, rs._data.primary_name)
    assert rs._data.new_view is not None
    assert rs._data.new_view == nvm


def test_new_view_by_quorums(lt_eq_gt):
    expected, replica_service = lt_eq_gt
    assert replica_service._data.new_view == expected

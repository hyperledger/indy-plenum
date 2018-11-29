import pytest

from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change

view_nos = [0, 2]


def test_id(param):
    return "view_" + str(param)


@pytest.fixture(scope="function", params=view_nos, ids=test_id)
def view_no(request, looper, txnPoolNodeSet):
    while txnPoolNodeSet[0].viewNo < request.param:
        ensure_view_change(looper, txnPoolNodeSet)
        ensureElectionsDone(looper, txnPoolNodeSet)
    assert txnPoolNodeSet[0].viewNo == request.param
    return request.param

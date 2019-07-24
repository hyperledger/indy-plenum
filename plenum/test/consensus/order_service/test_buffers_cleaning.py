import pytest

from plenum.common.util import get_utc_epoch
from plenum.server.replica import Replica
from plenum.test.testing_utils import FakeSomething


GLOBAL_VIEW_NO = 2


@pytest.fixture
def initial_view_no():
    return GLOBAL_VIEW_NO


def test_ordered_cleaning(orderer):
    orderer._data.view_no = GLOBAL_VIEW_NO
    total = []

    num_requests_per_view = 3
    for viewNo in range(GLOBAL_VIEW_NO + 1):
        for seqNo in range(num_requests_per_view):
            reqId = viewNo, seqNo
            orderer.l_addToOrdered(*reqId)
            total.append(reqId)

    # gc is called after stable checkpoint, since no request executed
    # in this test starting it manually
    orderer.l_gc(100)
    # Requests with view lower then previous view
    # should not be in ordered
    assert len(orderer.ordered) == len(total[num_requests_per_view:])


def test_primary_names_cleaning(orderer):
    orderer._data.view_no = 0
    orderer.primary_names.clear()

    orderer.primary_name = "Node1:0"
    assert list(orderer.primary_names.items()) == \
           [(0, "Node1:0")]

    orderer._data.view_no += 1
    orderer.primary_name = "Node2:0"
    assert list(orderer.primary_names.items()) == \
           [(0, "Node1:0"), (1, "Node2:0")]

    orderer._data.view_no += 1
    orderer.primary_name = "Node3:0"
    assert list(orderer.primary_names.items()) == \
           [(1, "Node2:0"), (2, "Node3:0")]

    orderer._data.view_no += 1
    orderer.primary_name = "Node4:0"
    assert list(orderer.primary_names.items()) == \
           [(2, "Node3:0"), (3, "Node4:0")]
import pytest

from common.exceptions import PlenumValueError
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.utils import get_original_viewno

nodeCount = 4


@pytest.fixture(params=[0, 10])
def initial_view_no(request):
    return request.param


def test_is_next_pre_prepare(orderer):
    pp_view_no = 2
    pp_seq_no = 2
    orderer.last_ordered_3pc = (1, 3)

    assert orderer.view_no != pp_view_no
    assert not orderer._is_next_pre_prepare(pp_view_no, pp_seq_no)


def test_order_3pc_key(orderer):
    with pytest.raises(ValueError) as excinfo:
        orderer._order_3pc_key((1, 1))
    assert ("no PrePrepare with a 'key' {} found"
            .format((1, 1))) in str(excinfo.value)


@pytest.mark.skip(reason="dequeue_prepares not implemented yet")
def test_can_pp_seq_no_be_in_view(orderer):
    view_no = orderer.view_no + 1
    assert orderer.view_no < view_no
    with pytest.raises(PlenumValueError) as excinfo:
        orderer._can_pp_seq_no_be_in_view(view_no, 1)
    assert ("expected: <= current view_no {}"
            .format(orderer.view_no)) in str(excinfo.value)


def test_is_msg_from_primary_doesnt_crash_on_msg_with_view_greater_than_current(orderer):
    class FakeMsg:
        def __init__(self, viewNo):
            self.viewNo = viewNo

    invalid_view_no = 1 if orderer.view_no is None else orderer.view_no + 1

    # This shouldn't crash
    orderer._is_msg_from_primary(FakeMsg(invalid_view_no), "some_sender")


def test_request_prepare_doesnt_crash_when_primary_is_not_connected(orderer):
    orderer._data.primary_name = 'Omega:0'
    orderer._request_msg = lambda *args, **kwargs: None
    # This shouldn't crash
    orderer._request_prepare((0, 1))


def test_pp_storages_ordering(pre_prepare, orderer: OrderingService):
    orderer.generate_pp_digest(pre_prepare.reqIdr,
                               get_original_viewno(pre_prepare),
                               pre_prepare.ppTime)



def test_pp_storages_ordering(pre_prepare, orderer):
    orderer._preprepare_batch(pre_prepare)
    assert orderer._data.preprepared
    assert not orderer._data.prepared

    orderer._prepare_batch(pre_prepare)
    assert orderer._data.preprepared
    assert orderer._data.prepared

    orderer._clear_batch(pre_prepare)
    assert not orderer._data.preprepared
    assert not orderer._data.prepared

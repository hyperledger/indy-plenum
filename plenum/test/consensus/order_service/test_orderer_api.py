import pytest

from common.exceptions import PlenumValueError

nodeCount = 4


@pytest.fixture(params=[0, 10])
def initial_view_no(request):
    return request.param


def test_is_next_pre_prepare(orderer):
    pp_view_no = 2
    pp_seq_no = 1
    orderer.last_ordered_3pc = (1, 2)

    assert orderer.view_no != pp_view_no
    assert not orderer.l__is_next_pre_prepare(pp_view_no, pp_seq_no)


def test_order_3pc_key(orderer):
    with pytest.raises(ValueError) as excinfo:
        orderer.l_order_3pc_key((1, 1))
    assert ("no PrePrepare with a 'key' {} found"
            .format((1, 1))) in str(excinfo.value)


@pytest.mark.skip(reason="dequeue_prepares not implemented yet")
def test_can_pp_seq_no_be_in_view(orderer):
    view_no = orderer.view_no + 1
    assert orderer.view_no < view_no
    with pytest.raises(PlenumValueError) as excinfo:
        orderer.l_can_pp_seq_no_be_in_view(view_no, 1)
    assert ("expected: <= current view_no {}"
            .format(orderer.view_no)) in str(excinfo.value)


def test_is_msg_from_primary_doesnt_crash_on_msg_with_view_greater_than_current(orderer):
    class FakeMsg:
        def __init__(self, viewNo):
            self.viewNo = viewNo

    invalid_view_no = 1 if orderer.view_no is None else orderer.view_no + 1

    # This shouldn't crash
    orderer.l_isMsgFromPrimary(FakeMsg(invalid_view_no), "some_sender")


def test_request_prepare_doesnt_crash_when_primary_is_not_connected(orderer):
    orderer.primary_name = 'Omega:0'
    orderer._request_msg = lambda *args, **kwargs: None
    # This shouldn't crash
    orderer._request_prepare((0, 1))

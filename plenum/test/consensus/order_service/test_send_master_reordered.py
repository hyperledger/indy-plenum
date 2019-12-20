import pytest

from plenum.common.messages.internal_messages import MasterReorderedAfterVC

PREV_VIEW_PP_CERT = 10


@pytest.fixture(params=[(0, 5), (1, 10), (2, 15)])
def view_pp(request):
    return request.param


def test_send_master_reordered(orderer, view_pp):
    view_no, pp_seq_no = view_pp
    orderer._data.prev_view_prepare_cert = PREV_VIEW_PP_CERT
    orderer._add_to_ordered(view_no, pp_seq_no)
    if pp_seq_no == PREV_VIEW_PP_CERT:
        assert len(orderer._bus.sent_messages) == 1
        assert isinstance(orderer._bus.sent_messages[0], MasterReorderedAfterVC)

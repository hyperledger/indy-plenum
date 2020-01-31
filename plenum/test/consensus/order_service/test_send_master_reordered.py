import pytest

from plenum.common.messages.internal_messages import MasterReorderedAfterVC
from plenum.common.messages.node_messages import Commit

PREV_VIEW_PP_CERT = 10


@pytest.fixture(params=[((0, 5), False),
                        ((1, 9), False),
                        ((1, 10), False),
                        ((1, 11), True),
                        ((1, 12), False),
                        ((1, 15), False)])
def view_pp(request):
    return request.param


def test_send_master_reordered_for_non_ordered(orderer, view_pp, is_master):
    (view_no, pp_seq_no), should_be_sent = view_pp
    orderer._data.is_master = is_master
    orderer._data.prev_view_prepare_cert = PREV_VIEW_PP_CERT
    orderer._try_finish_reordering_after_vc(pp_seq_no)
    if should_be_sent and is_master:
        assert len(orderer._bus.sent_messages) == 1
        assert isinstance(orderer._bus.sent_messages[0], MasterReorderedAfterVC)
    else:
        assert len(orderer._bus.sent_messages) == 0


def test_send_master_reordered_for_already_ordered(orderer, view_pp, is_master):
    (view_no, pp_seq_no), should_be_sent = view_pp
    orderer._data.is_master = is_master
    orderer._data.prev_view_prepare_cert = PREV_VIEW_PP_CERT
    orderer._data.last_ordered_3pc = (view_no, 10000)

    commit = Commit(0, view_no, pp_seq_no)
    orderer._add_to_commits(commit, "Alpha")
    if should_be_sent and is_master:
        assert len(orderer._bus.sent_messages) == 1
        assert isinstance(orderer._bus.sent_messages[0], MasterReorderedAfterVC)
    else:
        assert len(orderer._bus.sent_messages) == 0

import pytest

@pytest.fixture(params=[0, 10])
def view_no(request):
    return request.param


@pytest.fixture(params=[10, 20])
def pp_seq_no(request):
    return request.param


@pytest.fixture()
def last_ordered_3pc(view_no, pp_seq_no):
    return (view_no, pp_seq_no)


def test_increment_last_ordered_affects_to_lastPrePrepareSeqNo(orderer, is_master,
                                                               last_ordered_3pc, view_no, pp_seq_no):
    incremented_pp_seq_no = pp_seq_no + 1
    orderer._data.is_master = is_master
    orderer._lastPrePrepareSeqNo = pp_seq_no
    orderer._data.last_ordered_3pc = last_ordered_3pc
    orderer.last_ordered_3pc = (view_no, incremented_pp_seq_no)
    assert orderer.lastPrePrepareSeqNo == incremented_pp_seq_no


def test_decrement_last_ordered_does_not_affects_to_lastPrePrepareSeqNo(orderer, is_master,
                                                                        last_ordered_3pc, view_no, pp_seq_no):
    decremented_pp_seq_no = pp_seq_no - 1
    orderer._data.is_master = is_master
    orderer._lastPrePrepareSeqNo = pp_seq_no
    orderer._data.last_ordered_3pc = last_ordered_3pc
    orderer.last_ordered_3pc = (view_no, decremented_pp_seq_no)
    assert orderer.lastPrePrepareSeqNo != decremented_pp_seq_no
    assert orderer.lastPrePrepareSeqNo == pp_seq_no

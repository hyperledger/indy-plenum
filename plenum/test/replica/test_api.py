import pytest

from common.exceptions import LogicError, PlenumValueError
from plenum.test.helper import create_pre_prepare_no_bls

nodeCount = 4


def test_is_next_pre_prepare(replica):
    pp_view_no = 2
    pp_seq_no = 1
    replica._last_ordered_3pc = (1, 2)

    assert replica.viewNo != pp_view_no
    with pytest.raises(LogicError) as excinfo:
        replica._Replica__is_next_pre_prepare(pp_view_no, pp_seq_no)
    assert (("{} is not equal to current view_no {}"
             .format(pp_view_no, replica.viewNo)) in str(excinfo.value))


def test_last_prepared_certificate_in_view(replica):
    replica.isMaster = False
    with pytest.raises(LogicError) as excinfo:
        replica.last_prepared_certificate_in_view()
    assert "is not a master" in str(excinfo.value)


def test_order_3pc_key(replica):
    with pytest.raises(ValueError) as excinfo:
        replica.order_3pc_key((1, 1))
    assert ("no PrePrepare with a 'key' {} found"
            .format((1, 1))) in str(excinfo.value)


def test_can_pp_seq_no_be_in_view(replica):
    view_no = replica.viewNo + 1
    assert replica.viewNo < view_no
    with pytest.raises(PlenumValueError) as excinfo:
        replica.can_pp_seq_no_be_in_view(view_no, 1)
    assert ("expected: <= current view_no {}"
            .format(replica.viewNo)) in str(excinfo.value)


def test_is_msg_from_primary_doesnt_crash_on_msg_with_view_greater_than_current(replica):
    class FakeMsg:
        def __init__(self, viewNo):
            self.viewNo = viewNo

    invalid_view_no = 1 if replica.viewNo is None else replica.viewNo + 1

    # This shouldn't crash
    replica.isMsgFromPrimary(FakeMsg(invalid_view_no), "some_sender")


def test_remove_stashed_checkpoints_doesnt_crash_when_current_view_no_is_greater_than_last_stashed_checkpoint(replica):
    till_3pc_key = (1, 1)
    replica.stashedRecvdCheckpoints[1] = {till_3pc_key: {}}
    setattr(replica.node, 'viewNo', 2)

    # This shouldn't crash
    replica._remove_stashed_checkpoints(till_3pc_key)


def test_last_prepared_none_if_no_prepares(replica):
    """
    There is no any prepares for this replica. In that case we expect,
    that last_prepares_sertificate will return None
    """
    assert len(replica.prepares) == 0
    assert replica.last_prepared_certificate_in_view() is None


def test_last_prepared_sertificate_return_max_3PC_key(replica):
    """

    All the prepares has enough quorum. Expected result is that last_prepared_sertificate
    must be Max3PCKey(all of prepare's keys) == (0, 2)
    """
    replica.prepares.clear()
    prepare1 = create_pre_prepare_no_bls(state_root='8J7o1k3mDX2jtBvgVfFbijdy6NKbfeJ7SfY3K1nHLzQB',
                                         view_no=0, pp_seq_no=1)
    prepare1.voters = ('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0')
    replica.prepares[(0, 1)] = prepare1

    prepare2 = create_pre_prepare_no_bls(state_root='EuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMAZ',
                                         view_no=0, pp_seq_no=2)
    prepare2.voters = ('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0')
    replica.prepares[(0, 2)] = prepare2
    assert replica.last_prepared_certificate_in_view() == (0, 2)


def test_lst_sertificate_return_max_3PC_key_of_quorumed_prepare(replica):
    """

    Prepare with key (0, 2) does not have quorum of prepare.
    Therefore, expected Max3PC key must be (0, 1), because of previous prepare has enough quorum
    """
    replica.isMaster = True
    replica.prepares.clear()
    prepare1 = create_pre_prepare_no_bls(state_root='8J7o1k3mDX2jtBvgVfFbijdy6NKbfeJ7SfY3K1nHLzQB',
                                         view_no=0, pp_seq_no=1)
    prepare1.voters = ('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0')
    replica.prepares[(0, 1)] = prepare1
    prepare2 = create_pre_prepare_no_bls(state_root='EuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMAZ',
                                         view_no=0, pp_seq_no=2)
    prepare2.voters = ('Delta:0',)
    replica.prepares[(0, 2)] = prepare2
    assert replica.last_prepared_certificate_in_view() == (0, 1)


def test_request_prepare_doesnt_crash_when_primary_is_not_connected(replica):
    replica.primaryName = 'Omega:0'
    replica.node.request_msg = lambda t, d, r: None
    # This shouldn't crash
    replica._request_prepare((0, 1))

import pytest

from common.exceptions import LogicError, PlenumValueError
from plenum.common.messages.node_messages import Prepare
from plenum.server.models import ThreePhaseVotes
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.test.testing_utils import FakeSomething


nodeCount = 4


@pytest.fixture(scope='module')
def replica(tconf):
    node = FakeSomething(
        name="fake node",
        ledger_ids=[0],
        viewNo=0,
        quorums=Quorums(nodeCount)
    )
    bls_bft_replica = FakeSomething(
        gc=lambda *args: None,
    )
    replica = Replica(
        node, instId=0, isMaster=False,
        config=tconf, bls_bft_replica=bls_bft_replica
    )
    return replica


def test_view_change_done(replica):
    with pytest.raises(LogicError) as excinfo:
        replica.on_view_change_done()
    assert "is not a master" in str(excinfo.value)


def test_on_propagate_primary_done(replica):
    with pytest.raises(LogicError) as excinfo:
        replica.on_propagate_primary_done()
    assert "is not a master" in str(excinfo.value)


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
    with pytest.raises(LogicError) as excinfo:
        replica.last_prepared_certificate_in_view()
    assert "is not a master" in str(excinfo.value)


def test_order_3pc_key(replica):
    with pytest.raises(ValueError) as excinfo:
        replica.order_3pc_key((1, 1))
    assert ("no PrePrepare with a 'key' {} found"
            .format((1, 1))) in str(excinfo.value)


def test_can_pp_seq_no_be_in_view(replica):
    view_no = 1
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


def test_last_prepared_sertificate_return_max_3PC_key(replica):
    """

    All the prepares has enough quorum. Expected result is that last_prepared_sertificate
    must be Max3PCKey(all of prepare's keys) == (0, 2)
    """
    replica.isMaster = True
    replica.prepares.clear()
    replica.prepares[(0, 1)] = ThreePhaseVotes(voters=('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0'),
                                               msg=Prepare(digest='962c916c01b3e306748a3fdc8f2bf6a6f97f9db5330b56daa32df9c163b36d48',
                                                           instId=0,
                                                           ppSeqNo=1,
                                                           ppTime=1530603633,
                                                           stateRootHash='8J7o1k3mDX2jtBvgVfFbijdy6NKbfeJ7SfY3K1nHLzQB',
                                                           txnRootHash='Hhyw96wihpeG9whMNuyPhUcTV76HiHcYJrepDsjuarYJ',
                                                           viewNo=0))
    replica.prepares[(0, 2)] = ThreePhaseVotes(voters=('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0'),
                                               msg=Prepare(digest='12a05a12df55d4595807ec6edaf3bc36766feb4ab5479a5b45434a4288c9871b',
                                                           instId=0,
                                                           ppSeqNo=1,
                                                           ppTime=1530603633,
                                                           stateRootHash='EuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMAZ',
                                                           txnRootHash='2WfbH1TvYXrALiyRfgKr137siPFYveNrsb2LjjKjgwsE',
                                                           viewNo=0))
    assert replica.last_prepared_certificate_in_view() == (0, 2)



def test_lst_sertificate_return_max_of_quorumed_prepare(replica):
    """

    Prepare with key (0, 2) does not have quorum of prepare.
    Therefore, expected Max3PC key must be (0, 1), because of previous prepare has enough quorum
    """
    replica.isMaster = True
    replica.prepares.clear()
    replica.prepares[(0, 1)] = ThreePhaseVotes(voters=('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0'),
                                               msg=Prepare(digest='962c916c01b3e306748a3fdc8f2bf6a6f97f9db5330b56daa32df9c163b36d48',
                                                           instId=0,
                                                           ppSeqNo=1,
                                                           ppTime=1530603633,
                                                           stateRootHash='8J7o1k3mDX2jtBvgVfFbijdy6NKbfeJ7SfY3K1nHLzQB',
                                                           txnRootHash='Hhyw96wihpeG9whMNuyPhUcTV76HiHcYJrepDsjuarYJ',
                                                           viewNo=0))
    replica.prepares[(0, 2)] = ThreePhaseVotes(voters=('Delta:0',),
                                               msg=Prepare(digest='12a05a12df55d4595807ec6edaf3bc36766feb4ab5479a5b45434a4288c9871b',
                                                           instId=0,
                                                           ppSeqNo=1,
                                                           ppTime=1530603633,
                                                           stateRootHash='EuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMAZ',
                                                           txnRootHash='2WfbH1TvYXrALiyRfgKr137siPFYveNrsb2LjjKjgwsE',
                                                           viewNo=0))
    assert replica.last_prepared_certificate_in_view() == (0, 1)

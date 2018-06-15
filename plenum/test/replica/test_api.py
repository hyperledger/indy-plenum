import pytest

from common.exceptions import LogicError, PlenumValueError
from plenum.server.replica import Replica
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(scope='module')
def replica(tconf):
    node = FakeSomething(
        name="fake node",
        ledger_ids=[0],
        viewNo=0
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
    replica._last_ordered_3pc = (1,2)

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
        replica.order_3pc_key((1,1))
    assert ("no PrePrepare with a 'key' {} found"
            .format((1,1))) in str(excinfo.value)

def test_can_pp_seq_no_be_in_view(replica):
    view_no = 1
    assert replica.viewNo < view_no
    with pytest.raises(PlenumValueError) as excinfo:
        replica.can_pp_seq_no_be_in_view(view_no, 1)
    assert ("expected: <= current view_no {}"
            .format(replica.viewNo)) in str(excinfo.value)

import pytest

from common.exceptions import LogicError, PlenumValueError
from plenum.test.helper import create_pre_prepare_no_bls

nodeCount = 4


def test_last_prepared_certificate_in_view(replica):
    replica._consensus_data.is_master = False
    with pytest.raises(LogicError) as excinfo:
        replica._ordering_service.l_last_prepared_certificate_in_view()
    assert "is not a master" in str(excinfo.value)


def test_last_prepared_none_if_no_prepares(replica):
    """
    There is no any prepares for this replica. In that case we expect,
    that last_prepares_sertificate will return None
    """
    assert len(replica._ordering_service.prepares) == 0
    assert replica._ordering_service.l_last_prepared_certificate_in_view() is None


def test_last_prepared_sertificate_return_max_3PC_key(replica):
    """

    All the prepares has enough quorum. Expected result is that last_prepared_sertificate
    must be Max3PCKey(all of prepare's keys) == (0, 2)
    """
    replica._ordering_service.prepares.clear()
    prepare1 = create_pre_prepare_no_bls(state_root='8J7o1k3mDX2jtBvgVfFbijdy6NKbfeJ7SfY3K1nHLzQB',
                                         view_no=0, pp_seq_no=1)
    prepare1.voters = ('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0')
    replica._ordering_service.prepares[(0, 1)] = prepare1

    prepare2 = create_pre_prepare_no_bls(state_root='EuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMAZ',
                                         view_no=0, pp_seq_no=2)
    prepare2.voters = ('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0')
    replica._ordering_service.prepares[(0, 2)] = prepare2
    assert replica._ordering_service.l_last_prepared_certificate_in_view() == (0, 2)


def test_lst_sertificate_return_max_3PC_key_of_quorumed_prepare(replica):
    """

    Prepare with key (0, 2) does not have quorum of prepare.
    Therefore, expected Max3PC key must be (0, 1), because of previous prepare has enough quorum
    """
    replica.isMaster = True
    replica._ordering_service.prepares.clear()
    prepare1 = create_pre_prepare_no_bls(state_root='8J7o1k3mDX2jtBvgVfFbijdy6NKbfeJ7SfY3K1nHLzQB',
                                         view_no=0, pp_seq_no=1)
    prepare1.voters = ('Alpha:0', 'Beta:0', 'Gamma:0', 'Delta:0')
    replica._ordering_service.prepares[(0, 1)] = prepare1
    prepare2 = create_pre_prepare_no_bls(state_root='EuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMAZ',
                                         view_no=0, pp_seq_no=2)
    prepare2.voters = ('Delta:0',)
    replica._ordering_service.prepares[(0, 2)] = prepare2
    assert replica._ordering_service.l_last_prepared_certificate_in_view() == (0, 1)

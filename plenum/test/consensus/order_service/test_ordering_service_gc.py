from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root


def test_clear_old_view_pre_prepares_till_3pc(orderer):
    orderer.old_view_preprepares[(0, 1, 'd1')] = create_pre_prepare_no_bls(generate_state_root(),
                                                                           view_no=0, pp_seq_no=1)
    orderer.old_view_preprepares[(0, 2, 'd2')] = create_pre_prepare_no_bls(generate_state_root(),
                                                                           view_no=0, pp_seq_no=2)
    orderer.old_view_preprepares[(1, 3, 'd3')] = create_pre_prepare_no_bls(generate_state_root(),
                                                                           view_no=1, pp_seq_no=3)
    orderer.old_view_preprepares[(1, 4, 'd4')] = create_pre_prepare_no_bls(generate_state_root(),
                                                                           view_no=1, pp_seq_no=4)
    orderer.old_view_preprepares[(2, 5, 'd5')] = create_pre_prepare_no_bls(generate_state_root(),
                                                                           view_no=1, pp_seq_no=5)

    orderer.gc(till3PCKey=(1, 3))

    assert len(orderer.old_view_preprepares) == 2
    assert (1, 4, 'd4') in orderer.old_view_preprepares
    assert (2, 5, 'd5') in orderer.old_view_preprepares


def test_clear_old_view_pre_prepares_till_3pc_multiple_digests(orderer):
    pp1 = create_pre_prepare_no_bls(generate_state_root(), view_no=0, pp_seq_no=1)
    orderer.old_view_preprepares[(0, 1, 'd1')] = pp1
    orderer.old_view_preprepares[(0, 1, 'd2')] = pp1

    pp2 = create_pre_prepare_no_bls(generate_state_root(), view_no=1, pp_seq_no=2)
    orderer.old_view_preprepares[(1, 2, 'd1')] = pp2
    orderer.old_view_preprepares[(1, 2, 'd2')] = pp2

    orderer.gc(till3PCKey=(1, 1))

    assert len(orderer.old_view_preprepares) == 2
    assert (1, 2, 'd1') in orderer.old_view_preprepares
    assert (1, 2, 'd2') in orderer.old_view_preprepares

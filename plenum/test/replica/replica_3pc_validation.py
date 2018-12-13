import pytest

from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root, create_commit_no_bls_sig, create_prepare


@pytest.fixture(scope='module')
def validator():
    return ThreePCValidator(inst_id=1)


@pytest.fixture(scope='module', params=[0, 1])
def inst_id(request):
    return request.param


@pytest.fixture(scope='module', params=[0, 1])
def view_no(request):
    return request.param


@pytest.fixture(scope='module', params=[0, 1])
def pp_seq_no(request):
    return request.param


@pytest.fixture(scope='module', params=['PRE-PREPARE', 'PREPARE', 'COMMIT'])
def threepc_msg(request, inst_id, view_no, pp_seq_no):
    if request.param == 'PRE-PREPARE':
        return create_pre_prepare_no_bls(generate_state_root(),
                                         view_no=view_no,
                                         pp_seq_no=pp_seq_no,
                                         inst_id=inst_id)
    if request.param == 'PREPARE':
        return create_prepare(req_key=(view_no, pp_seq_no),
                              state_root=generate_state_root(),
                              inst_id=inst_id)
    if request.param == 'COMMIT':
        return create_commit_no_bls_sig(req_key=(view_no, pp_seq_no),
                                        inst_id=inst_id)


def test_check_view_no(validator, threepc_msg):
    assert validator.validate(threepc_msg)

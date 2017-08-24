import pytest
from charm.core.math.pairing import pc_element
from crypto.bls.charm.bls_charm import BlsSignatureCharm


@pytest.fixture()
def default_params():
    return BlsSignatureCharm.default_params()


@pytest.fixture()
def bls1(default_params):
    return BlsSignatureCharm(*default_params)


@pytest.fixture()
def bls2(default_params):
    return BlsSignatureCharm(*default_params)


@pytest.fixture()
def bls1_with_keys(bls1):
    sk, pk = bls1.generate_keys()
    return bls1, pk


@pytest.fixture()
def bls2_with_keys(bls2):
    sk, pk = bls2.generate_keys()
    return bls2, pk


def test_default_params(default_params):
    group_name, g = default_params
    assert group_name == 'MNT224'
    assert isinstance(g, pc_element)
    bls = BlsSignatureCharm(group_name, g)


def test_new(bls1, default_params):
    assert bls1.group
    assert bls1.g == default_params[1]


def test_generate_keys(bls1):
    sk, pk = bls1.generate_keys()
    assert sk
    assert isinstance(sk, pc_element)
    assert pk
    assert isinstance(pk, pc_element)
    assert sk != pk


def test_sign(bls1_with_keys):
    bls = bls1_with_keys[0]
    sig = bls.sign('Hello!')
    assert sig


def test_multi_sign(bls1_with_keys):
    bls = bls1_with_keys[0]
    sigs = []
    sigs.append(bls.sign('Hello!'))
    sigs.append(bls.sign('Hello!'))
    sigs.append(bls.sign('Hello!!!!!'))
    sig = bls.create_multi_sig(sigs)
    assert sig


def test_verify_one_signature(bls1_with_keys, bls2_with_keys):
    bls1, pk1 = bls1_with_keys
    bls2, pk2 = bls2_with_keys

    sig1 = bls1.sign('Hello!')
    sig2 = bls2.sign('Hello!')

    assert bls2.verify_sig(sig1, 'Hello!', pk1)
    assert bls1.verify_sig(sig2, 'Hello!', pk2)
    assert bls1.verify_sig(sig1, 'Hello!', pk1)
    assert bls2.verify_sig(sig2, 'Hello!', pk2)


def test_verify_multi_signature(bls1_with_keys, bls2_with_keys):
    bls1, pk1 = bls1_with_keys
    bls2, pk2 = bls2_with_keys

    msgs = []
    msgs.append(('Hello!', pk1))
    msgs.append(('Hello!', pk2))

    sigs = []
    sigs.append(bls1.sign(msgs[0][0]))
    sigs.append(bls2.sign(msgs[1][0]))

    multi_sig1 = bls1.create_multi_sig(sigs)
    multi_sig2 = bls1.create_multi_sig(sigs)

    assert bls1.verify_multi_sig(multi_sig1, msgs)
    assert bls1.verify_multi_sig(multi_sig2, msgs)
    assert bls2.verify_multi_sig(multi_sig1, msgs)
    assert bls2.verify_multi_sig(multi_sig2, msgs)

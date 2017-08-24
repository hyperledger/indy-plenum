import pytest
from charm.core.math.pairing import pc_element
from crypto.bls.charm.bls_crypto_charm import BlsCryptoCharm, BlsGroupParamsLoaderCharmHardcoded, BlsSerializerCharm


@pytest.fixture()
def default_params():
    params_loader = BlsGroupParamsLoaderCharmHardcoded()
    return params_loader.load_group_params()


@pytest.fixture()
def keys(default_params):
    return BlsCryptoCharm.generate_keys(default_params)


@pytest.fixture()
def serializer(default_params):
    return BlsSerializerCharm(default_params)


@pytest.fixture()
def bls1(keys, default_params, serializer):
    return BlsCryptoCharm(*keys, default_params, serializer)


@pytest.fixture()
def bls2(keys, default_params, serializer):
    return BlsCryptoCharm(*keys, default_params, serializer)


def test_default_params(default_params):
    group_name, g = default_params
    assert group_name == 'MNT224'
    assert isinstance(g, pc_element)


def test_generate_keys(default_params):
    sk, pk = BlsCryptoCharm.generate_keys(default_params)
    assert sk
    assert isinstance(sk, pc_element)
    assert pk
    assert isinstance(pk, pc_element)
    assert sk != pk


def test_serialize(keys, serializer):
    sk, pk = keys
    assert sk == serializer.deserialize(serializer.serialize(sk))
    assert pk == serializer.deserialize(serializer.serialize(pk))


def test_new(keys, default_params, serializer):
    bls = BlsCryptoCharm(*keys, default_params, serializer)
    assert bls.group
    assert bls.g == default_params[1]


def test_sign(bls1):
    sig = bls1.sign('Hello!')
    assert sig


def test_multi_sign(bls1):
    sigs = []
    sigs.append(bls1.sign('Hello!'))
    sigs.append(bls1.sign('Hello!'))
    sigs.append(bls1.sign('Hello!!!!!'))
    sig = bls1.create_multi_sig(sigs)
    assert sig


def test_verify_one_signature(bls1, bls2):
    pk1 = bls1.pk
    pk2 = bls2.pk

    sig1 = bls1.sign('Hello!')
    sig2 = bls2.sign('Hello!')

    assert bls2.verify_sig(sig1, 'Hello!', pk1)
    assert bls1.verify_sig(sig2, 'Hello!', pk2)
    assert bls1.verify_sig(sig1, 'Hello!', pk1)
    assert bls2.verify_sig(sig2, 'Hello!', pk2)


def test_verify_multi_signature(bls1, bls2):
    pk1 = bls1.pk
    pk2 = bls2.pk

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

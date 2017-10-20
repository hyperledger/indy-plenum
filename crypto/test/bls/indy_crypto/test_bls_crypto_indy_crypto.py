import pytest
from crypto.bls.indy_crypto.bls_crypto_indy_crypto import BlsGroupParamsLoaderIndyCrypto, \
    BlsCryptoSignerIndyCrypto, BlsCryptoVerifierIndyCrypto


@pytest.fixture()
def default_params():
    params_loader = BlsGroupParamsLoaderIndyCrypto()
    return params_loader.load_group_params()


@pytest.fixture()
def keys1(default_params):
    return BlsCryptoSignerIndyCrypto.generate_keys(default_params)


@pytest.fixture()
def keys2(default_params):
    return BlsCryptoSignerIndyCrypto.generate_keys(default_params)


@pytest.fixture()
def bls_signer1(keys1, default_params):
    return BlsCryptoSignerIndyCrypto(*keys1, default_params)


@pytest.fixture()
def bls_signer2(keys2, default_params):
    return BlsCryptoSignerIndyCrypto(*keys2, default_params)


@pytest.fixture()
def bls_verifier(default_params):
    return BlsCryptoVerifierIndyCrypto(default_params)


def test_default_params(default_params):
    group_name, g = default_params
    assert group_name == 'generator'
    assert isinstance(g, str)


def test_generate_keys_no_seed(default_params):
    sk, pk = BlsCryptoSignerIndyCrypto.generate_keys(default_params)
    assert sk
    assert isinstance(sk, str)
    assert pk
    assert isinstance(pk, str)
    assert sk != pk


def test_generate_keys_str_seed_48bit(default_params):
    seed = 'Seed' + '0' * (48 - len('Seed'))
    sk, pk = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed)
    assert sk
    assert isinstance(sk, str)
    assert pk
    assert isinstance(pk, str)
    assert sk != pk


def test_generate_keys_str_seed_32bit(default_params):
    seed = 'Seed' + '0' * (32 - len('Seed'))
    sk, pk = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed)
    assert sk
    assert isinstance(sk, str)
    assert pk
    assert isinstance(pk, str)
    assert sk != pk


def test_generate_keys_bytes_seed_48bit(default_params):
    seed = 'Seed' + '0' * (48 - len('Seed'))
    seed = seed.encode()
    sk, pk = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed)
    assert sk
    assert isinstance(sk, str)
    assert pk
    assert isinstance(pk, str)
    assert sk != pk


def test_generate_keys_bytes_seed_32bit(default_params):
    seed = 'Seed' + '0' * (32 - len('Seed'))
    seed = seed.encode()
    sk, pk = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed)
    assert sk
    assert isinstance(sk, str)
    assert pk
    assert isinstance(pk, str)
    assert sk != pk


def test_generate_keys_str_seed_32bit_for_nodes(default_params):
    seeds = []
    nodes_count = 4
    for i in range(1, nodes_count + 1):
        name = "Node" + str(i)
        seed = ('0' * (32 - len(name)) + name)
        seeds.append(seed)

    pks = set()
    for i in range(nodes_count):
        sk, pk = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seeds[i])
        pks.add(pk)
        print(pk)

    assert len(pks) == nodes_count


def test_generate_different_keys(default_params):
    seed2 = 'Seed' + '0' * (48 - len('Seed'))
    seed3 = 'seeeed' + '0' * (48 - len('seeeed'))

    sk1, pk1 = BlsCryptoSignerIndyCrypto.generate_keys(default_params)
    sk3, pk3 = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed2)
    sk4, pk4 = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed3)
    assert sk1 != sk3 != sk4
    assert pk1 != pk3 != pk4


def test_sign(bls_signer1):
    sig = bls_signer1.sign('Hello!')
    assert sig


def test_multi_sign(bls_signer1, bls_verifier):
    sigs = []
    sigs.append(bls_signer1.sign('Hello!'))
    sigs.append(bls_signer1.sign('Hello!'))
    sigs.append(bls_signer1.sign('Hello!!!!!'))
    sig = bls_verifier.create_multi_sig(sigs)
    assert sig


def test_verify_one_signature(bls_signer1, bls_signer2, bls_verifier):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    sig1 = bls_signer1.sign('Hello!')
    sig2 = bls_signer2.sign('Hello!')

    assert bls_verifier.verify_sig(sig1, 'Hello!', pk1)
    assert bls_verifier.verify_sig(sig2, 'Hello!', pk2)


def test_verify_multi_signature(bls_signer1, bls_signer2, bls_verifier):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    msg = 'Hello!'
    pks = [pk1, pk2]

    sigs = []
    sigs.append(bls_signer1.sign(msg))
    sigs.append(bls_signer2.sign(msg))

    multi_sig11 = bls_verifier.create_multi_sig(sigs)
    multi_sig12 = bls_verifier.create_multi_sig(sigs)

    assert bls_verifier.verify_multi_sig(multi_sig11, msg, pks)
    assert bls_verifier.verify_multi_sig(multi_sig12, msg, pks)

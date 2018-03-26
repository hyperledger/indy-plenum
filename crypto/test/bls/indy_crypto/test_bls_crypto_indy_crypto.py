import base58
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


@pytest.fixture()
def message():
    return 'Hello!'.encode()


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


@pytest.yield_fixture(scope="function", params=['48', '32', '31', '33'])
def seed(request):
    seed_len = int(request.param)
    return 'Seed' + '0' * (seed_len - len('Seed'))


def test_generate_keys_str_seed(default_params, seed):
    sk, pk = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed)
    assert sk
    assert isinstance(sk, str)
    assert pk
    assert isinstance(pk, str)
    assert sk != pk


def test_generate_keys_bytes_seed(default_params, seed):
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
    seed2 = 'Seed' + '0' * (32 - len('Seed'))
    seed3 = 'seeeed' + '0' * (32 - len('seeeed'))
    seed4 = 'Seed' + '0' * (31 - len('Seed'))

    sk1, pk1 = BlsCryptoSignerIndyCrypto.generate_keys(default_params)
    sk2, pk2 = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed2)
    sk3, pk3 = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed3)
    sk4, pk4 = BlsCryptoSignerIndyCrypto.generate_keys(default_params, seed4)
    assert sk1 != sk2 != sk3 != sk4
    assert pk1 != pk2 != pk3 != pk4


def test_sign(bls_signer1):
    sig = bls_signer1.sign('Hello!'.encode())
    assert sig


def test_multi_sign(bls_signer1, bls_verifier):
    sigs = []
    sigs.append(bls_signer1.sign('Hello!'.encode()))
    sigs.append(bls_signer1.sign('Hello!'.encode()))
    sigs.append(bls_signer1.sign('Hello!!!!!'.encode()))
    sig = bls_verifier.create_multi_sig(sigs)
    assert sig


def test_verify_one_signature(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    sig1 = bls_signer1.sign(message)
    sig2 = bls_signer2.sign(message)

    assert bls_verifier.verify_sig(sig1, message, pk1)
    assert bls_verifier.verify_sig(sig2, message, pk2)


def test_verify_one_signature_long_message(bls_signer1, bls_signer2, bls_verifier):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    msg = ('Hello!' * 1000000).encode()
    sig1 = bls_signer1.sign(msg)
    sig2 = bls_signer2.sign(msg)

    assert bls_verifier.verify_sig(sig1, msg, pk1)
    assert bls_verifier.verify_sig(sig2, msg, pk2)


def test_verify_non_base58_signature(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    assert not bls_verifier.verify_sig('Incorrect Signature 1',
                                       message,
                                       pk)


def test_verify_non_base58_pk(bls_signer1, bls_verifier, message):
    sig = bls_signer1.sign('Hello!')
    assert not bls_verifier.verify_sig(sig,
                                       message,
                                       'Incorrect pk 1')


def test_verify_non_base58_sig_and_pk(bls_verifier, message):
    assert not bls_verifier.verify_sig('Incorrect Signature 1',
                                       message,
                                       'Incorrect pk 1')


def invalid_values(valid_value):
    return [
        valid_value[:-2],
        valid_value[:-5],
        valid_value + '0',
        valid_value + base58.b58encode(b'0'),
        valid_value + base58.b58encode(b'somefake'),
        base58.b58encode(b'somefakevaluesomefakevalue')
    ]


def invalid_short_values(valid_value):
    return [
        valid_value[:1],
        valid_value[:2],
        valid_value[:5],
        '',
        base58.b58encode(b'1' * 10),
        base58.b58encode(b'1' * 2)
    ]


def invalid_long_values():
    return [
        base58.b58encode(b'1' * 500),
        base58.b58encode(b'1' * 1000),
        base58.b58encode(b'1' * 10000)
    ]


def test_verify_invalid_signature(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    sig = bls_signer1.sign(message)

    for invalid_sig in invalid_values(sig):
        assert not bls_verifier.verify_sig(invalid_sig,
                                           message, pk)


def test_verify_invalid_pk(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    sig = bls_signer1.sign(message)

    for invalid_pk in invalid_values(pk):
        assert not bls_verifier.verify_sig(sig,
                                           message, invalid_pk)


def test_verify_invalid_short_signature(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    sig = bls_signer1.sign(message)

    for invalid_sig in invalid_short_values(sig):
        assert not bls_verifier.verify_sig(invalid_sig,
                                           message, pk)


def test_verify_invalid_short_pk(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    sig = bls_signer1.sign(message)

    for invalid_pk in invalid_short_values(pk):
        assert not bls_verifier.verify_sig(sig,
                                           message, invalid_pk)


def test_verify_invalid_long_signature(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    for invalid_sig in invalid_long_values():
        assert not bls_verifier.verify_sig(invalid_sig,
                                           message, pk)


def test_verify_invalid_long_pk(bls_signer1, bls_verifier, message):
    sig = bls_signer1.sign(message)
    for invalid_pk in invalid_long_values():
        assert not bls_verifier.verify_sig(sig,
                                           message, invalid_pk)


def test_verify_multi_signature(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    pks = [pk1, pk2]

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig1 = bls_verifier.create_multi_sig(sigs)
    assert bls_verifier.verify_multi_sig(multi_sig1, message, pks)


def test_verify_multi_signature_long_message(bls_signer1, bls_signer2, bls_verifier):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    msg = ('Hello!' * 1000000).encode()
    pks = [pk1, pk2]

    sigs = []
    sigs.append(bls_signer1.sign(msg))
    sigs.append(bls_signer2.sign(msg))

    multi_sig = bls_verifier.create_multi_sig(sigs)
    assert bls_verifier.verify_multi_sig(multi_sig, msg, pks)


def test_verify_non_base_58_multi_signature(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    pks = [pk1, pk2]

    multi_sig = 'Incorrect multi signature 1'
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)


def test_verify_non_base_58_pk_multi_signature(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    pks = [pk1, 'Incorrect multi signature 1']
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = ['Incorrect multi signature 1', pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = ['Incorrect multi signature 1', 'Incorrect multi signature 2']
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)


def test_verify_invalid_multi_signature(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    pks = [pk1, pk2]

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    for invalid_multi_sig in invalid_values(multi_sig):
        assert not bls_verifier.verify_multi_sig(invalid_multi_sig,
                                                 message, pks)


def test_verify_invalid_multi_signature_short(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    pks = [pk1, pk2]

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    for invalid_multi_sig in invalid_short_values(multi_sig):
        assert not bls_verifier.verify_multi_sig(invalid_multi_sig,
                                                 message, pks)


def test_verify_invalid_multi_signature_long(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    pks = [pk1, pk2]

    for invalid_multi_sig in invalid_long_values():
        assert not bls_verifier.verify_multi_sig(invalid_multi_sig,
                                                 message, pks)


def test_verify_multi_signature_invalid_pk(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    for invalid_pk2 in invalid_values(pk2):
        pks = [pk1, invalid_pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)

    for invalid_pk1 in invalid_values(pk1):
        pks = [invalid_pk1, pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)

    for invalid_pk1, invalid_pk2 in zip(invalid_values(pk1),
                                        invalid_values(pk2)):
        pks = [invalid_pk1, invalid_pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)


def test_verify_multi_signature_invalid_short_pk(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    for invalid_pk2 in invalid_short_values(pk2):
        pks = [pk1, invalid_pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)

    for invalid_pk1 in invalid_short_values(pk1):
        pks = [invalid_pk1, pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)

    for invalid_pk1, invalid_pk2 in zip(invalid_short_values(pk1),
                                        invalid_short_values(pk2)):
        pks = [invalid_pk1, invalid_pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)


def test_verify_multi_signature_invalid_long_pk(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    for invalid_pk2 in invalid_long_values():
        pks = [pk1, invalid_pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)

    for invalid_pk1 in invalid_long_values():
        pks = [invalid_pk1, pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)

    for invalid_pk1, invalid_pk2 in zip(invalid_long_values(),
                                        invalid_long_values()):
        pks = [invalid_pk1, invalid_pk2]
        assert not bls_verifier.verify_multi_sig(multi_sig,
                                                 message, pks)

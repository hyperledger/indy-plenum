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


def test_verify_invalid_signature(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    sig = bls_signer1.sign(message)

    assert not bls_verifier.verify_sig(sig[:-2],
                                       message, pk)
    assert not bls_verifier.verify_sig(sig[:-5],
                                       message, pk)
    assert not bls_verifier.verify_sig(sig + '0',
                                       message, pk)
    assert not bls_verifier.verify_sig(sig + base58.b58encode(b'0'),
                                       message, pk)
    assert not bls_verifier.verify_sig(sig + base58.b58encode(b'somefake'),
                                       message, pk)
    assert not bls_verifier.verify_sig(base58.b58encode(b'somefakesignaturesomefakesignature'),
                                       message, pk)


def test_verify_invalid_pk(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    sig = bls_signer1.sign(message)

    assert not bls_verifier.verify_sig(sig,
                                       message, pk[:-2])
    assert not bls_verifier.verify_sig(sig,
                                       message, pk[:-5])
    assert not bls_verifier.verify_sig(sig,
                                       message, pk + '0')
    assert not bls_verifier.verify_sig(sig,
                                       message, pk + base58.b58encode(b'0'))
    assert not bls_verifier.verify_sig(sig,
                                       message, pk + base58.b58encode(b'somefake'))
    assert not bls_verifier.verify_sig(sig,
                                       message, base58.b58encode(b'somefakepksomefakepk'))


def test_verify_invalid_short_signature(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    sig = bls_signer1.sign(message)

    assert not bls_verifier.verify_sig(sig[:1],
                                       message, pk)
    assert not bls_verifier.verify_sig(sig[:2],
                                       message, pk)
    assert not bls_verifier.verify_sig(sig[:5],
                                       message, pk)
    assert not bls_verifier.verify_sig('',
                                       message, pk)
    assert not bls_verifier.verify_sig(base58.b58encode(b'1' * 10),
                                       message, pk)
    assert not bls_verifier.verify_sig(base58.b58encode(b'1' * 2),
                                       message, pk)


def test_verify_invalid_short_pk(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    sig = bls_signer1.sign(message)

    assert not bls_verifier.verify_sig(sig,
                                       message, pk[:1])
    assert not bls_verifier.verify_sig(sig,
                                       message, pk[:2])
    assert not bls_verifier.verify_sig(sig,
                                       message, pk[:5])
    assert not bls_verifier.verify_sig(sig,
                                       message, '')
    assert not bls_verifier.verify_sig(sig,
                                       message, base58.b58encode(b'1' * 10))
    assert not bls_verifier.verify_sig(sig,
                                       message, base58.b58encode(b'1' * 2))


def test_verify_invalid_long_signature(bls_signer1, bls_verifier, message):
    pk = bls_signer1.pk
    assert not bls_verifier.verify_sig(base58.b58encode(b'1' * 500),
                                       message, pk)
    assert not bls_verifier.verify_sig(base58.b58encode(b'1' * 1000),
                                       message, pk)
    assert not bls_verifier.verify_sig(base58.b58encode(b'1' * 10000),
                                       message, pk)


def test_verify_invalid_long_pk(bls_signer1, bls_verifier, message):
    sig = bls_signer1.sign(message)
    assert not bls_verifier.verify_sig(sig,
                                       message, base58.b58encode(b'1' * 500))
    assert not bls_verifier.verify_sig(sig,
                                       message, base58.b58encode(b'1' * 1000))
    assert not bls_verifier.verify_sig(sig,
                                       message, base58.b58encode(b'1' * 10000))


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

    assert not bls_verifier.verify_multi_sig(multi_sig[:-2],
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(multi_sig[:-5],
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(multi_sig + '0',
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(multi_sig + base58.b58encode(b'0'),
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(multi_sig + base58.b58encode(b'somefake'),
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(base58.b58encode(b'somefakesignaturesomefakesignature'),
                                             message, pks)


def test_verify_invalid_multi_signature_short(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    pks = [pk1, pk2]

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    assert not bls_verifier.verify_multi_sig(multi_sig[:1],
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(multi_sig[:2],
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(multi_sig[:5],
                                             message, pks)
    assert not bls_verifier.verify_multi_sig('',
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(base58.b58encode(b'1' * 10),
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(base58.b58encode(b'1' * 2),
                                             message, pks)


def test_verify_invalid_multi_signature_long(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    pks = [pk1, pk2]

    assert not bls_verifier.verify_multi_sig(base58.b58encode(b'1' * 500),
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(base58.b58encode(b'1' * 1000),
                                             message, pks)
    assert not bls_verifier.verify_multi_sig(base58.b58encode(b'1' * 10000),
                                             message, pks)


def test_verify_multi_signature_invalid_pk(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    pks = [pk1, pk2[:-2]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1[:-2], pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1[:-2], pk2[:-2]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = [pk1[-5], pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1, pk2[-5]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1[-5], pk2[-5]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = [pk1 + '0', pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1, pk2 + '0']
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1 + '0', pk2 + '0']
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = [pk1 + base58.b58encode(b'somefake'), pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1, pk2 + base58.b58encode(b'somefake')]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1 + base58.b58encode(b'somefake'), pk2 + base58.b58encode(b'somefake')]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)


def test_verify_multi_signature_invalid_short_pk(bls_signer1, bls_signer2, bls_verifier, message):
    pk1 = bls_signer1.pk
    pk2 = bls_signer2.pk

    sigs = []
    sigs.append(bls_signer1.sign(message))
    sigs.append(bls_signer2.sign(message))

    multi_sig = bls_verifier.create_multi_sig(sigs)

    pks = [pk1, '']
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = ['', pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = ['', '']
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = [pk1[:1], pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1, pk2[:1]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1[:1], pk2[:1]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = [pk1[:2], pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1, pk2[:2]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1[:2], pk2[:2]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = [pk1[:5], pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1, pk2[:5]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1[:5], pk2[:5]]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = [base58.b58encode(b'1' * 10), pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1, base58.b58encode(b'1' * 10)]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [base58.b58encode(b'1' * 10), base58.b58encode(b'1' * 10)]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

    pks = [base58.b58encode(b'1' * 2), pk2]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [pk1, base58.b58encode(b'1' * 2)]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)
    pks = [base58.b58encode(b'1' * 2), base58.b58encode(b'1' * 2)]
    assert not bls_verifier.verify_multi_sig(multi_sig, message, pks)

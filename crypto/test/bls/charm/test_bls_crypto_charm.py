# import pytest
# from charm.core.math.pairing import pc_element
# from crypto.bls.charm.bls_crypto_charm import BlsCryptoCharm, BlsGroupParamsLoaderCharmHardcoded, BlsSerializerCharm
#
#
# @pytest.fixture()
# def default_params():
#     params_loader = BlsGroupParamsLoaderCharmHardcoded()
#     return params_loader.load_group_params()
#
#
# @pytest.fixture()
# def keys(default_params):
#     return BlsCryptoCharm.generate_keys(default_params)
#
#
# @pytest.fixture()
# def bls1(keys, default_params):
#     return BlsCryptoCharm(*keys, default_params)
#
#
# @pytest.fixture()
# def bls2(keys, default_params):
#     return BlsCryptoCharm(*keys, default_params)
#
# @pytest.skip
# def test_default_params(default_params):
#     group_name, g = default_params
#     assert group_name == 'MNT224'
#     assert isinstance(g, pc_element)
#
#
# def test_generate_keys_no_seed(default_params):
#     sk, pk = BlsCryptoCharm.generate_keys(default_params)
#     assert sk
#     assert isinstance(sk, str)
#     assert pk
#     assert isinstance(pk, str)
#     assert sk != pk
#
#
# def test_generate_keys_int_seed(default_params):
#     seed = 123456789
#     sk, pk = BlsCryptoCharm.generate_keys(default_params, seed)
#     assert sk
#     assert isinstance(sk, str)
#     assert pk
#     assert isinstance(pk, str)
#     assert sk != pk
#
#
# def test_generate_keys_str_seed(default_params):
#     seed = 'Seed' + '0' * (32 - len('Seed'))
#     sk, pk = BlsCryptoCharm.generate_keys(default_params, seed)
#     assert sk
#     assert isinstance(sk, str)
#     assert pk
#     assert isinstance(pk, str)
#     assert sk != pk
#
#
# def test_generate_keys_bytes_seed(default_params):
#     seed = 'Seed' + '0' * (32 - len('Seed'))
#     seed = seed.encode()
#     sk, pk = BlsCryptoCharm.generate_keys(default_params, seed)
#     assert sk
#     assert isinstance(sk, str)
#     assert pk
#     assert isinstance(pk, str)
#     assert sk != pk
#
#
# def test_generate_different_keys(default_params):
#     seed1 = 123456789
#     seed2 = 'Seed' + '0' * (32 - len('Seed'))
#     seed3 = 'seeeed'.encode()
#
#     sk1, pk1 = BlsCryptoCharm.generate_keys(default_params)
#     sk2, pk2 = BlsCryptoCharm.generate_keys(default_params, seed1)
#     sk3, pk3 = BlsCryptoCharm.generate_keys(default_params, seed2)
#     sk4, pk4 = BlsCryptoCharm.generate_keys(default_params, seed3)
#     assert sk1 != sk2 != sk3 != sk4
#     assert pk1 != pk2 != pk3 != pk4
#
#
# def test_serialize(keys, default_params):
#     serializer = BlsSerializerCharm(default_params)
#     sk, pk = keys
#     assert sk == serializer.deserialize_from_bytes(serializer.serialize_to_bytes(sk))
#     assert pk == serializer.deserialize_from_bytes(serializer.serialize_to_bytes(pk))
#     assert sk == serializer.deserialize_from_str(serializer.serialize_to_str(sk))
#     assert pk == serializer.deserialize_from_str(serializer.serialize_to_str(pk))
#
#
# def test_new(keys, default_params):
#     bls = BlsCryptoCharm(*keys, default_params)
#     assert bls.group
#     assert bls.g == default_params[1]
#
#
# def test_sign(bls1):
#     sig = bls1.sign('Hello!')
#     assert sig
#
#
# def test_multi_sign(bls1):
#     sigs = []
#     sigs.append(bls1.sign('Hello!'))
#     sigs.append(bls1.sign('Hello!'))
#     sigs.append(bls1.sign('Hello!!!!!'))
#     sig = bls1.create_multi_sig(sigs)
#     assert sig
#
#
# def test_verify_one_signature(bls1, bls2):
#     pk1 = bls1.pk
#     pk2 = bls2.pk
#
#     sig1 = bls1.sign('Hello!')
#     sig2 = bls2.sign('Hello!')
#
#     assert bls2.verify_sig(sig1, 'Hello!', pk1)
#     assert bls1.verify_sig(sig2, 'Hello!', pk2)
#     assert bls1.verify_sig(sig1, 'Hello!', pk1)
#     assert bls2.verify_sig(sig2, 'Hello!', pk2)
#
#
# def test_verify_multi_signature(bls1, bls2):
#     pk1 = bls1.pk
#     pk2 = bls2.pk
#
#     msg = 'Hello!'
#     pks = [pk1, pk2]
#
#     sigs = []
#     sigs.append(bls1.sign(msg))
#     sigs.append(bls2.sign(msg))
#
#     multi_sig1 = bls1.create_multi_sig(sigs)
#     multi_sig2 = bls1.create_multi_sig(sigs)
#
#     assert bls1.verify_multi_sig(multi_sig1, msg, pks)
#     assert bls1.verify_multi_sig(multi_sig2, msg, pks)
#     assert bls2.verify_multi_sig(multi_sig1, msg, pks)
#     assert bls2.verify_multi_sig(multi_sig2, msg, pks)

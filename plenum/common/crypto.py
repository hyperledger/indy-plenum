import ctypes

from libnacl import crypto_box_SECRETKEYBYTES, nacl, crypto_box_PUBLICKEYBYTES


def ed25519SkToCurve25519(sk):
    secretKey = ctypes.create_string_buffer(crypto_box_SECRETKEYBYTES)
    ret = nacl.crypto_sign_ed25519_sk_to_curve25519(secretKey, sk)
    if ret:
        raise Exception("error in converting ed22519 key to curve25519")
    return secretKey.raw


def ed25519PkToCurve25519(pk):
    publicKey = ctypes.create_string_buffer(crypto_box_PUBLICKEYBYTES)
    ret = nacl.crypto_sign_ed25519_pk_to_curve25519(publicKey, pk)
    if ret:
        raise Exception("error in converting ed22519 key to curve25519")
    return publicKey.raw


def getEd25519AndCurve25519Keys(seed=None):

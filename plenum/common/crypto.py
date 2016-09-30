import ctypes
from binascii import unhexlify, hexlify

from libnacl import crypto_box_SECRETKEYBYTES, nacl, crypto_box_PUBLICKEYBYTES
from plenum.common.util import cleanSeed, isHex
from raet.nacling import Signer


def ed25519SkToCurve25519(sk, toHex=False):
    if isHex(sk):
        sk = unhexlify(sk)
    secretKey = ctypes.create_string_buffer(crypto_box_SECRETKEYBYTES)
    ret = nacl.crypto_sign_ed25519_sk_to_curve25519(secretKey, sk)
    if ret:
        raise Exception("error in converting ed22519 key to curve25519")
    return hexlify(secretKey.raw) if toHex else secretKey.raw


def ed25519PkToCurve25519(pk, toHex=False):
    if isHex(pk):
        pk = unhexlify(pk)
    publicKey = ctypes.create_string_buffer(crypto_box_PUBLICKEYBYTES)
    ret = nacl.crypto_sign_ed25519_pk_to_curve25519(publicKey, pk)
    if ret:
        raise Exception("error in converting ed22519 key to curve25519")
    return hexlify(publicKey.raw) if toHex else publicKey.raw


def getEd25519AndCurve25519Keys(seed=None):
    if seed:
        seed = cleanSeed(seed)
    signer = Signer(seed)
    sigkey, verkey = signer.keyhex, signer.verhex
    prikey, pubkey = hexlify(ed25519SkToCurve25519(signer.keyraw)), \
                     hexlify(ed25519PkToCurve25519(signer.verraw))
    return (sigkey, verkey), (prikey, pubkey)

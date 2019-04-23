import ctypes
import random
import string
from binascii import unhexlify, hexlify

import base58
from libnacl import crypto_box_SECRETKEYBYTES, nacl, crypto_box_PUBLICKEYBYTES
from common.exceptions import PlenumValueError
from stp_core.crypto.nacl_wrappers import Signer

# TODO: move it to crypto repo


# TODO returning a None when a None is passed is non-obvious; refactor
def cleanSeed(seed=None):
    if seed:
        bts = seedFromHex(seed)
        if not bts:
            if isinstance(seed, str):
                seed = seed.encode('utf-8')
            bts = bytes(seed)
            if len(seed) != 32:
                raise PlenumValueError('seed', seed, '64-length hexadecimal string or 32-length in bytes representation')
        return bts


# TODO this behavior is non-obvious; refactor
def seedFromHex(seed):
    if len(seed) == 64:
        try:
            return unhexlify(seed)
        except Exception:
            pass


def isHex(val: str) -> bool:
    """
    Return whether the given str represents a hex value or not

    :param val: the string to check
    :return: whether the given str represents a hex value
    """
    if isinstance(val, bytes):
        # only decodes utf-8 string
        try:
            val = val.decode()
        except ValueError:
            return False
    return isinstance(val, str) and all(c in string.hexdigits for c in val)


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
    prikey, pubkey = hexlify(ed25519SkToCurve25519(signer.keyraw)), hexlify(ed25519PkToCurve25519(signer.verraw))
    return (sigkey, verkey), (prikey, pubkey)


def base58_is_correct_ed25519_key(key):
    try:
        ed25519PkToCurve25519(base58.b58decode(key))
    except Exception:
        return False
    return True


def randomSeed(size=32):
    return ''.join(random.choice(string.hexdigits)
                   for _ in range(size)).encode()


def isHexKey(key):
    try:
        return len(key) == 64 and isHex(key)
    except ValueError as ex:
        return False
    except Exception as ex:
        raise ex

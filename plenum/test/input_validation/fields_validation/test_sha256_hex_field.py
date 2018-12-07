import base64
import hashlib
from binascii import hexlify

import base58

from plenum.common.messages.fields import Sha256HexField

validator = Sha256HexField()
msg = b'some message'


def test_sha256_invalid_hex_field():
    assert validator.validate(
        '') == 'not a valid hash (needs to be in hex too)'
    assert validator.validate(1) == "expected types 'str', got 'int'"


def test_sha256_valid_hex_field():
    assert validator.validate(hashlib.sha256(msg).hexdigest()) is None


def test_only_sha256_field():
    # Any other hashing algo like sha512 or md5 not allowed, only sha256
    h512 = hashlib.sha512(msg)
    hex_h512 = h512.hexdigest()
    assert validator.validate(
        hex_h512) == 'not a valid hash (needs to be in hex too)'
    hmd5 = hashlib.md5(msg)
    hex_hmd5 = hmd5.hexdigest()
    assert validator.validate(
        hex_hmd5) == 'not a valid hash (needs to be in hex too)'

    # Only hex representation of sha256 will work
    h256 = hashlib.sha256(msg)
    hex_h256 = h256.hexdigest()
    assert validator.validate(hex_h256) is None


def test_only_sha256_hex_field():
    h256 = hashlib.sha256(msg)

    # Base64 or base58 representations will not work, only hex will
    b64_h256 = base64.b64encode(h256.digest()).decode()
    assert validator.validate(
        b64_h256) == 'not a valid hash (needs to be in hex too)'
    b58_h256 = base58.b58encode(h256.digest()).decode("utf-8")
    assert validator.validate(
        b58_h256) == 'not a valid hash (needs to be in hex too)'

    assert validator.validate(hexlify(h256.digest()).decode()) is None

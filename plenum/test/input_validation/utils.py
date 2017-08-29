import base58

from plenum.test.input_validation.constants import VALID_BASE58


def assert_valid(smth):
    assert not smth


def assert_invalid(smth):
    assert smth


B58_BY_BYTE_LENGTH = {
    len(base58.b58decode(VALID_BASE58[:i])):
        VALID_BASE58[:i] for i in range(1, len(VALID_BASE58) + 1)
}


def b58_by_len(byte_length):
    return B58_BY_BYTE_LENGTH.get(byte_length)

import pytest
import string
from plenum.common.messages.fields import VerkeyField
from plenum.common.util import randomString
from plenum.test.input_validation.utils import *


valid_base58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyzzzzaaaaa'
validator = VerkeyField()


def test_verkey_valid_long():
    assert not validator.validate('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghij')


def test_verkey_invalid_long():
    assert validator.validate('~123456999CDEFGHJKLMNPQRSTUVWXYZabcdefghij')
    assert validator.validate('123456789ABCDEFGHJKLMNPQRSbcdefghijqqqqqqqqqqqqqqqqqq')



def test_verkey_valid_short():
    assert not validator.validate('~EFGHJKLMNPQRSTabcdefghij')


def test_verkey_invalid_short():
    assert validator.validate('EFGHJKLMNPQRSTabcdefghij')
    assert validator.validate('EFGHJKLMNPQRSTabcdefghijqqqqqqq')
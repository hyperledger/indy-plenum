import pytest

from common.version import DigitDotVersion
from plenum.common.messages.fields import VersionField
from plenum.config import VERSION_FIELD_LIMIT


class VersionTestCls(DigitDotVersion):
    def __init__(self, version: str, **kwargs):
        super().__init__(version, parts_num=(2, 3), **kwargs)


max_length = 20
validator = VersionField(version_cls=VersionTestCls, max_length=max_length)


def test_empty_version():
    assert validator.validate('')


def test_valid_version():
    assert not validator.validate('1.2.3')
    assert not validator.validate('0.2.0')
    assert not validator.validate('0.2')


def test_one_component_fails():
    assert validator.validate('123')


def test_a_string_component_fails():
    assert validator.validate('asdf.asdf')


def test_invalid_version():
    assert validator.validate('123.ads.00')


def test_invalid_number_of_comp():
    assert validator.validate('1.2.3.4')


def test_invalid_negative_comp():
    assert validator.validate('-1.-2.-3')
    assert validator.validate('-1.2.3')
    assert validator.validate('1.2.-3')


def test_spaces():
    assert validator.validate(' 1.2.3')
    assert validator.validate('1. 2.3')
    assert validator.validate('1.2. 3')
    assert validator.validate('1 .2.3')
    assert validator.validate('1.2 .3')
    assert validator.validate('1.2.3 ')
    assert validator.validate(' 1 .2.3')
    assert validator.validate('1. 2 .3')
    assert validator.validate('1.2. 3 ')
    assert validator.validate(' -1.2.3')
    assert validator.validate('1. -2 .3')
    assert validator.validate('1.2.-3 ')


def test_max_length_limit():
    assert validator.validate("1" * (VERSION_FIELD_LIMIT + 1))
    assert validator.validate("{}.{}".format("1" * (VERSION_FIELD_LIMIT + 1), "2"))
    assert validator.validate("{}.{}".format("2", "1" * (VERSION_FIELD_LIMIT + 1)))
    assert validator.validate("{}.{}".format("1" * int(VERSION_FIELD_LIMIT / 2), "2" * int(VERSION_FIELD_LIMIT / 2)))

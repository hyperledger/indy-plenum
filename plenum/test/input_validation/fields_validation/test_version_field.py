import pytest

from plenum.common.version import DigitDotVersion
from plenum.common.messages.fields import VersionField
from plenum.config import VERSION_FIELD_LIMIT


class VersionTestCls(DigitDotVersion):
    def __init__(self, version: str, **kwargs):
        super().__init__(version, parts_num=(2, 3), **kwargs)


max_length = 20
legacy_validator = VersionField(components_number=(2, 3,), max_length=max_length)
validator = VersionField(version_cls=VersionTestCls, max_length=max_length)


@pytest.fixture(
    params=(legacy_validator, validator),
    ids=('legacy', 'actual')
)
def validator(request):
    return request.param


def test_empty_version(validator):
    assert validator.validate('')


def test_valid_version(validator):
    assert not validator.validate('1.2.3')
    assert not validator.validate('0.2.0')
    assert not validator.validate('0.2')


def test_one_component_fails(validator):
    assert validator.validate('123')


def test_a_string_component_fails(validator):
    assert validator.validate('asdf.asdf')


def test_invalid_version(validator):
    assert validator.validate('123.ads.00')


def test_invalid_number_of_comp(validator):
    assert validator.validate('1.2.3.4')


def test_invalid_negative_comp(validator):
    assert validator.validate('-1.-2.-3')
    assert validator.validate('-1.2.3')
    assert validator.validate('1.2.-3')


def test_spaces(validator):
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


def test_max_length_limit(validator):
    assert validator.validate("1" * (VERSION_FIELD_LIMIT + 1))
    assert validator.validate("{}.{}".format("1" * (VERSION_FIELD_LIMIT + 1), "2"))
    assert validator.validate("{}.{}".format("2", "1" * (VERSION_FIELD_LIMIT + 1)))
    assert validator.validate("{}.{}".format("1" * int(VERSION_FIELD_LIMIT / 2), "2" * int(VERSION_FIELD_LIMIT / 2)))

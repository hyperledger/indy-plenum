import pytest

from plenum.common.messages.fields import VersionField


validator = VersionField()


def test_empty_version():
    assert validator.validate('')


def test_valid_version():
    assert not validator.validate('1.2.3')
    assert not validator.validate('0.2.0')
    assert not validator.validate('0.2')


def test_invalid_version():
    assert validator.validate('123')
    assert validator.validate('asdf')
    assert validator.validate('123.ads.00')
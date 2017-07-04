import pytest

from plenum.common.messages.fields import VersionField


validator = VersionField(components_number=(2, 3,))


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
import pytest
from common.serializers.field import Field


def testCorrectField():
    Field("field1", str, str)
    Field("field1", str, int)
    Field("field1", int, str)
    Field("field1", str, float)
    Field("field1", str, bool)


def testNoName():
    with pytest.raises(AssertionError):
        Field(None, str, str)


def testInvalidName():
    with pytest.raises(AssertionError):
        Field(1111, str, str)


def testNoEncoder():
    with pytest.raises(AssertionError):
        Field("field1", None, str)


def testInvalidEncoder():
    with pytest.raises(AssertionError):
        Field("field1", "encoder", str)


def testNoDecoder():
    with pytest.raises(AssertionError):
        Field("field1", str, None)


def testInvalidDecoder():
    with pytest.raises(AssertionError):
        Field("field1", str, "decoder")

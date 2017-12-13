from collections import OrderedDict

import pytest
from common.serializers.compact_serializer import CompactSerializer

fields = OrderedDict([
    ("f1", (str, str)),
    ("f2", (str, str)),
    ("f3", (str, int)),
    ("f4", (str, float)),
    ("f5", (str, eval))
])


@pytest.fixture(scope='function')
def serializer():
    return CompactSerializer(fields)


def testInitCompactSerializerWithCorrectFileds():
    CompactSerializer(fields)


def testInitCompactSerializerNoFileds():
    CompactSerializer()


def testInitCompactSerializerEmptyFileds():
    fields = OrderedDict([])
    CompactSerializer(fields)

# TODO: add tests to distinguish None and empty values


def testSerializeSimpleJson(serializer):
    assert b"v1|v2|3|4.0|True" == \
           serializer.serialize(
               {"f1": "v1", "f2": "v2", "f3": 3, "f4": 4.0, "f5": True}
           )


def testDeserializeSimpleJson(serializer):
    assert {"f1": "v1", "f2": "v2", "f3": 3, "f4": 4.0, "f5": True} == \
        serializer.deserialize(b"v1|v2|3|4.0|True")


def testSerializeDeserializeSimpleJson(serializer):
    json = {"f1": "v1", "f2": "v2", "f3": 3, "f4": 4.0, "f5": True}
    serialized = serializer.serialize(json)
    deserialized = serializer.deserialize(serialized)

    assert json == deserialized


def testSerializeToBytes(serializer):
    assert b"v1|v2|3|4.0|True" == \
           serializer.serialize(
               {"f1": "v1", "f2": "v2", "f3": 3, "f4": 4.0, "f5": True},
               toBytes=True
           )


def testSerializeToString(serializer):
    assert "v1|v2|3|4.0|True" == \
           serializer.serialize(
               {"f1": "v1", "f2": "v2", "f3": 3, "f4": 4.0, "f5": True},
               toBytes=False
           )


def testDeserializeFromBytes(serializer):
    assert {"f1": "v1", "f2": "v2", "f3": 3, "f4": 4.0, "f5": True} == \
        serializer.deserialize(b"v1|v2|3|4.0|True")


def testSerializeToFields(serializer):
    newFields = OrderedDict([
        ("ff1", (str, str)),
        ("ff2", (str, str)),
        ("ff3", (str, int))
    ])
    assert b"v1|v2|3" == \
           serializer.serialize(
               {"ff1": "v1", "ff2": "v2", "ff3": 3},
               fields=newFields
           )


def testDeserializeFromString(serializer):
    assert {"f1": "v1", "f2": "v2", "f3": 3, "f4": 4.0, "f5": True} == \
        serializer.deserialize("v1|v2|3|4.0|True")


def testDeserializeForFields(serializer):
    newFields = OrderedDict([
        ("ff1", (str, str)),
        ("ff2", (str, str)),
        ("ff3", (str, int))
    ])
    assert {"ff1": "v1", "ff2": "v2", "ff3": 3} == \
        serializer.deserialize("v1|v2|3", fields=newFields)


def testSerializeLessFields(serializer):
    assert b"|v1|2||" == serializer.serialize({"f2": "v1", "f3": 2})
    assert b"v1||||" == serializer.serialize({"f1": "v1"})
    assert b"||||" == serializer.serialize({})
    assert b"||||3" == serializer.serialize({"f5": 3})
    assert b"v1||||3" == serializer.serialize({"f1": "v1", "f5": 3})


def testDeserializeLessFields(serializer):
    assert {"f1": None, "f2": "v1", "f3": 2, "f4": None,
            "f5": None} == serializer.deserialize(b"|v1|2||")
    assert {"f1": "v1", "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"v1||||")
    assert {"f1": None, "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"|||||")
    assert {"f1": None, "f2": None, "f3": None, "f4": None,
            "f5": True} == serializer.deserialize(b"||||True")
    assert {"f1": "v1", "f2": None, "f3": None, "f4": None,
            "f5": False} == serializer.deserialize(b"v1||||False")


def testSerializeLessFieldsWithNone(serializer):
    assert b"|v1|2||" == serializer.serialize(
        {"f1": None, "f2": "v1", "f3": 2})
    assert b"v1||||" == serializer.serialize(
        {"f1": "v1", "f2": None, "f3": None, "f4": None, "f5": None})
    assert b"||||" == serializer.serialize(
        {"f1": None, "f2": None, "f3": None, "f4": None, "f5": None})
    assert b"||||3" == serializer.serialize({"f1": None, "f5": 3})
    assert b"||||3" == serializer.serialize(
        {"f1": None, "f2": None, "f3": None, "f4": None, "f5": 3})
    assert b"v1||||3" == serializer.serialize(
        {"f1": "v1", "f2": None, "f3": None, "f4": None, "f5": 3})
    assert b"v1||||3" == serializer.serialize(
        {"f1": "v1", "f3": None, "f5": 3})


def testSerializeInAnyOrder(serializer):
    assert b"|v1|2||" == serializer.serialize(
        OrderedDict([("f3", 2), ("f2", "v1")]))
    assert b"v1||||3" == serializer.serialize(
        OrderedDict([("f5", 3), ("f1", "v1")]))
    assert b"v1|v2|3|4.0|True" == \
           serializer.serialize(
               OrderedDict([("f2", "v2"), ("f5", True),
                            ("f3", 3), ("f1", "v1"), ("f4", 4.0)])
           )


def testSerializeSubfields():
    fields = OrderedDict([
        ("f1.a", (str, str)),
        ("f1.b", (str, int)),
        ("f1.c", (str, float)),
        ("f2.d", (str, str)),
        ("f2.e", (str, int)),
        ("f2.f", (str, float)),

    ])
    serializer = CompactSerializer(fields)

    json = {
        "f1": {"a": "v1", "b": 2, "c": 3.0},
        "f2": {"d": "v1", "e": 3, "f": 4.0},
    }
    assert b"v1|2|3.0|v1|3|4.0" == serializer.serialize(json)


def testDeserializeSubfields():
    fields = OrderedDict([
        ("f1.a", (str, str)),
        ("f1.b", (str, int)),
        ("f1.c", (str, float)),
        ("f2.d", (str, str)),
        ("f2.e", (str, int)),
        ("f2.f", (str, float)),

    ])
    serializer = CompactSerializer(fields)

    json = {
        "f1": {"a": "v1", "b": 2, "c": 3.0},
        "f2": {"d": "v1", "e": 3, "f": 4.0},
    }
    assert json == serializer.deserialize(b"v1|2|3.0|v1|3|4.0")


def testSerializeWrongFields(serializer):
    assert b"||||" == serializer.serialize({"wrongField": "v1"})
    assert b"||||" == serializer.serialize(
        {"wrongField1": "v1", "wrongField2": "v2"})
    assert b"v1|v2|3|4.0|True" == \
           serializer.serialize(
               {"f1": "v1", "f2": "v2", "f3": 3, "f4": 4.0,
                   "f5": True, "wrongField": "vvv"}
           )
    assert b"v1|v2|3|4.0|True" == \
           serializer.serialize(
               {"wrongField": "vvv", "f1": "v1", "f2": "v2",
                   "f3": 3, "f4": 4.0, "f5": True}
           )
    assert b"v1||||" == \
           serializer.serialize(
               {"wrongField": "vvv", "f1": "v1"}
           )


def testDeserializeNewFieldsAdded(serializer):
    assert {"f1": "v1", "f2": "v2", "f3": 2, "f4": None,
            "f5": None} == serializer.deserialize(b"v1|v2|2")
    assert {"f1": "v1", "f2": "v2", "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"v1|v2|")
    assert {"f1": "v1", "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"v1||")
    assert {"f1": None, "f2": None, "f3": 3, "f4": None,
            "f5": None} == serializer.deserialize(b"||3")
    assert {"f1": None, "f2": "v2", "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"|v2|")

    assert {"f1": None, "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"||")
    assert {"f1": None, "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"")
    assert {"f1": None, "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"|")
    assert {"f1": None, "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"||||")


def testDeserializeFieldsRemoved(serializer):
    assert {"f1": "v1", "f2": "v2", "f3": 2, "f4": 4.0,
            "f5": True} == serializer.deserialize(b"v1|v2|2|4.0|True|fff")
    assert {
        "f1": "v1",
        "f2": "v2",
        "f3": 2,
        "f4": 4.0,
        "f5": True} == serializer.deserialize(b"v1|v2|2|4.0|True|fff|dddd|eeee")
    assert {"f1": "v1", "f2": "v2", "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"v1|v2||||gggg")
    assert {"f1": "v1", "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"v1|||||||||||||")
    assert {"f1": None, "f2": None, "f3": 3, "f4": None,
            "f5": None} == serializer.deserialize(b"||3||||||sdsd|||eee")
    assert {"f1": None, "f2": "v2", "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"|v2|||||||dfdds||sdsd|")

    assert {"f1": None, "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"||||||")
    assert {"f1": None, "f2": None, "f3": None, "f4": None,
            "f5": None} == serializer.deserialize(b"|||||||||||||||")

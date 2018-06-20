import base58
from common.serializers.mapping_serializer import MappingSerializer


class Base58Serializer(MappingSerializer):
    def serialize(self, data, fields=None, toBytes=False):
        return base58.b58encode(data).decode("utf-8")

    def deserialize(self, data, fields=None):
        return base58.b58decode(data)

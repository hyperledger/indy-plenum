import base64
from common.serializers.mapping_serializer import MappingSerializer


class Base64Serializer(MappingSerializer):
    def serialize(self, data, fields=None, toBytes=False):
        return base64.b64encode(data)

    def deserialize(self, data, fields=None):
        return base64.b64decode(data)

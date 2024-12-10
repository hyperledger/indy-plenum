# Consider using bson or ubjson for serializing json


import base64
import json
from typing import Dict

from common.serializers.mapping_serializer import MappingSerializer


class OrderedJsonEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        kwargs['ensure_ascii'] = False
        kwargs['sort_keys'] = True
        kwargs['separators'] = (',', ':')
        super().__init__(*args, **kwargs)

    def encode(self, o):
        if isinstance(o, (bytes, bytearray)):
            return '"{}"'.format(base64.b64encode(o).decode("utf-8"))
        else:
            return super().encode(o)


JsonEncoder = OrderedJsonEncoder()


class JsonSerializer(MappingSerializer):
    """
    Class to convert a mapping to json with keys ordered in lexicographical
    order
    """

    @staticmethod
    def dumps(data, toBytes=True):
        encoded = JsonEncoder.encode(data)
        if toBytes:
            encoded = encoded.encode()
        return encoded

    @staticmethod
    def loads(data):
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()
        return json.loads(data)

    # The `fields` argument is kept to conform to the interface, its not
    # need in this method
    def serialize(self, data: Dict, fields=None, toBytes=True):
        return self.dumps(data, toBytes)

    # The `fields` argument is kept to conform to the interface, its not
    # need in this method
    def deserialize(self, data, fields=None):
        return self.loads(data)

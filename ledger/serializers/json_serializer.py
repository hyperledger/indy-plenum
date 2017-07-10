# Consider using bson or ubjson for serializing json


import base64
from typing import Dict

from ledger.serializers.mapping_serializer import MappingSerializer


try:
    import ujson as json
    from ujson import encode as uencode

    # Older versions of ujson's encode do not support `sort_keys`, if that
    # is the case default to using json
    uencode({'xx': '123', 'aa': 90}, sort_keys=True)

    class UJsonEncoder:
        @staticmethod
        def encode(o):
            if isinstance(o, (bytes, bytearray)):
                return '"{}"'.format(base64.b64encode(o).decode("utf-8"))
            else:
                return uencode(o, sort_keys=True)


    JsonEncoder = UJsonEncoder()

except (ImportError, TypeError):
    import json

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
                return json.JSONEncoder.encode(self, o)

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

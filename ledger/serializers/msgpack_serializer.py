from typing import Dict

import msgpack
from ledger.serializers.mapping_serializer import MappingSerializer
from storage.stream_serializer import StreamSerializer


class MsgPackSerializer(MappingSerializer, StreamSerializer):

    def serialize(self, data: Dict, fields=None, toBytes=True):
        return msgpack.packb(data, use_bin_type=True)

    def deserialize(self, data, fields=None):
        # TODO: it can be that we returned data by `get_lines`, that is already deserialized
        if not isinstance(data, (bytes, bytearray)):
            return data
        return msgpack.unpackb(data, encoding='utf-8')

    def get_lines(self, stream):
        return msgpack.Unpacker(stream, encoding='utf-8')
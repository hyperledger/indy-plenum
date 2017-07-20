from typing import Dict

import collections
import msgpack
from ledger.serializers.mapping_serializer import MappingSerializer
from storage.stream_serializer import StreamSerializer


def decode_to_sorted(obj):
    return collections.OrderedDict(obj)


class MsgPackSerializer(MappingSerializer, StreamSerializer):

    def serialize(self, data: Dict, fields=None, toBytes=True):
        if isinstance(data, Dict):
            data = collections.OrderedDict(sorted(data.items()))
        return msgpack.packb(data, use_bin_type=True)

    def deserialize(self, data, fields=None):
        # TODO: it can be that we returned data by `get_lines`, that is already deserialized
        if not isinstance(data, (bytes, bytearray)):
            return data
        return msgpack.unpackb(data, encoding='utf-8', object_pairs_hook=decode_to_sorted)

    def get_lines(self, stream):
        return msgpack.Unpacker(stream, encoding='utf-8', object_pairs_hook=decode_to_sorted)
import collections
from collections import OrderedDict
from typing import Dict

import msgpack
from common.serializers.mapping_serializer import MappingSerializer
from storage.stream_serializer import StreamSerializer


def decode_to_sorted(obj):
    return collections.OrderedDict(obj)


class MsgPackSerializer(MappingSerializer, StreamSerializer):

    def serialize(self, data: Dict, fields=None, toBytes=True):
        if isinstance(data, Dict):
            data = self.__sort_dict(data)
        return msgpack.packb(data, use_bin_type=True)

    def deserialize(self, data, fields=None):
        # TODO: it can be that we returned data by `get_lines`, that is already deserialized
        if not isinstance(data, (bytes, bytearray)):
            return data
        return msgpack.unpackb(data, encoding='utf-8', object_pairs_hook=decode_to_sorted)

    def get_lines(self, stream):
        return msgpack.Unpacker(stream, encoding='utf-8', object_pairs_hook=decode_to_sorted)

    def __sort_dict(self, dict) -> OrderedDict:
        dict = OrderedDict(sorted(dict.items()))
        for k,v in dict.items():
            if isinstance(v, Dict):
                dict[k] = self.__sort_dict(v)
        return dict
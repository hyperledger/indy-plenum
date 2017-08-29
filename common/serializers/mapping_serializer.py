class MappingSerializer:
    # TODO: Probably don't need `fields` here
    def serialize(self, data, fields=None, toBytes=False):
        raise NotImplementedError

    def deserialize(self, data, fields=None):
        raise NotImplementedError

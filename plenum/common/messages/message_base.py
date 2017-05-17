from operator import itemgetter

from typing import Mapping

from plenum.common.constants import OP_FIELD_NAME


class MessageBase(Mapping):
    typename = None
    schema = {}

    def __init__(self, *args, **kwargs):
        assert not (args and kwargs), '*args, **kwargs cannot be used together'
        if args:
            input_as_dict = dict(zip(map(itemgetter(0), self.schema), args))
        else:
            input_as_dict = kwargs
        schema_dict = dict(self.schema)
        missed_required_fields = set(self.__required_field_names) - set(input_as_dict)
        if missed_required_fields:
            raise TypeError("Missed fields: {}".format(', '.join(missed_required_fields)))
        for k, v in input_as_dict.items():
            if k not in schema_dict:
                raise TypeError("Unknown field: {}={}".format(k, v))
            validation_error = schema_dict[k].validate(v)
            if validation_error:
                raise TypeError("Field '{}' validation error: {}".format(k, validation_error))
        self.__fields = input_as_dict
        self.__fields[OP_FIELD_NAME] = self.typename

    @property
    def __required_field_names(self):
        required_fields = filter(lambda x: not x[1].optional, self.schema)
        return map(lambda x: x[0], required_fields)

    def __getattr__(self, item):
        if item in self.__fields:
            return self.__fields[item]
        raise AttributeError

    @property
    def _fields(self):
        """pretend a namedtuple (attribute _fields)"""
        return self.__fields

    @property
    def __dict__(self):
        """
        Return a dictionary form.
        """
        return self.__fields

    def __name__(self):
        return self.typename

    def __iter__(self):
        for f in self.__fields:
            yield f

    def __len__(self):
        return len(self.__fields)

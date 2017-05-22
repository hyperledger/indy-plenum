from operator import itemgetter

from typing import Mapping

from plenum.common.constants import OP_FIELD_NAME
from plenum.common.messages.fields import FieldValidator


class MessageValidator(FieldValidator):

    # the schema has to be an ordered iterable because the message class
    # can be create with positional arguments __init__(*args)
    schema = ()
    optional = False

    def validate(self, dct):
        self._validate_with_schema(dct, self.schema)

    def _validate_with_schema(self, dct, schema):
        schema_dct = dict(schema)
        missed_required_fields = set(self.__required_field_names) - set(dct)
        if missed_required_fields:
            self._raise_missed_fields(missed_required_fields)
        for k, v in dct.items():
            if k not in schema_dct:
                self._raise_unknown_fields(k, v)
            validation_error = schema_dct[k].validate(v)
            if validation_error:
                self._raise_invalid_fields(k, v, validation_error)

    @property
    def __required_field_names(self):
        required_fields = filter(lambda x: not x[1].optional, self.schema)
        return map(lambda x: x[0], required_fields)

    def _raise_missed_fields(self, *fields):
        raise TypeError("validation error: missed fields "
                        "'{}'".format(', '.join(map(str, fields))))

    def _raise_unknown_fields(self, field, value):
        raise TypeError("validation error: unknown field "
                        "({}={})".format(field, value))

    def _raise_invalid_fields(self, field, value, reason):
        raise TypeError("validation error: {} "
                        "({}={})".format(reason, field, value))


class MessageBase(Mapping, MessageValidator):
    typename = None

    def __init__(self, *args, **kwargs):
        assert not (args and kwargs), '*args, **kwargs cannot be used together'
        if args:
            input_as_dict = dict(zip(map(itemgetter(0), self.schema), args))
        else:
            input_as_dict = kwargs
        self.validate(input_as_dict)
        self._fields = input_as_dict
        self._fields[OP_FIELD_NAME] = self.typename

    def __getattr__(self, item):
        if item in self._fields:
            return self._fields[item]
        raise AttributeError

    @property
    def __dict__(self):
        """
        Return a dictionary form.
        """
        return self._fields

    @property
    def __name__(self):
        return self.typename

    def __iter__(self):
        for f in self._fields:
            yield f

    def __len__(self):
        return len(self._fields)

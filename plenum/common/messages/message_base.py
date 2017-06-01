from operator import itemgetter

import itertools
from typing import Mapping

from plenum.common.constants import OP_FIELD_NAME
from plenum.common.messages.fields import FieldValidator


class MessageValidator(FieldValidator):

    # the schema has to be an ordered iterable because the message class
    # can be create with positional arguments __init__(*args)
    schema = ()
    optional = False

    def validate(self, dct):
        self._validate_fields_with_schema(dct, self.schema)
        self._validate_message(dct)

    def _validate_fields_with_schema(self, dct, schema):
        if not isinstance(dct, dict):
            # TODO raise invalid type exception
            self._raise_invalid_fields('', dct, 'wrong type')
        schema_dct = dict(schema)
        required_fields = filter(lambda x: not x[1].optional, schema)
        required_field_names = map(lambda x: x[0], required_fields)
        missed_required_fields = set(required_field_names) - set(dct)
        if missed_required_fields:
            self._raise_missed_fields(*missed_required_fields)
        for k, v in dct.items():
            if k not in schema_dct:
                self._raise_unknown_fields(k, v)
            validation_error = schema_dct[k].validate(v)
            if validation_error:
                self._raise_invalid_fields(k, v, validation_error)

    def _validate_message(self, dct):
        return None

    def _raise_missed_fields(self, *fields):
        raise TypeError("validation error: missed fields "
                        "{}".format(', '.join(map(str, fields))))

    def _raise_unknown_fields(self, field, value):
        raise TypeError("validation error: unknown field "
                        "({}={})".format(field, value))

    def _raise_invalid_fields(self, field, value, reason):
        raise TypeError("validation error: {} "
                        "({}={})".format(reason, field, value))

    def _raise_invalid_message(self, reason):
        raise TypeError("validation error: {}".format(reason))


class MessageBase(Mapping, MessageValidator):
    typename = None

    def __init__(self, *args, **kwargs):
        assert not (args and kwargs), '*args, **kwargs cannot be used together'
        if args:
            input_as_dict = dict(zip(map(itemgetter(0), self.schema), args))
        else:
            input_as_dict = kwargs
        # remove op field before the validation procedure
        input_as_dict.pop(OP_FIELD_NAME, None)
        self.validate(input_as_dict)
        self._fields = [(k, input_as_dict[k]) for k, _ in self.schema if k in input_as_dict]

    def __getattr__(self, item):
        for k, v in self._fields:
            if item == k:
                return v
        raise AttributeError

    def __getitem__(self, key):
        if isinstance(key, slice):
            r = range(key.start or 0, min([len(self), key.stop or len(self)]), key.step or 1)
            return [self._fields[i][1] for i in r]
        elif isinstance(key, int):
            return self._fields[key][1]
        else:
            raise TypeError("Invalid argument type.")

    def _asdict(self):
        """
        Legacy form TaggedTuple
        """
        return self.__dict__

    @property
    def __dict__(self):
        """
        Return a dictionary form.
        """
        return dict(self._fields + [(OP_FIELD_NAME, self.typename)])

    @property
    def __name__(self):
        return self.typename

    def __iter__(self):
        for k, v in self._fields:
            yield v

    def __len__(self):
        return len(self._fields)

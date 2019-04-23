from collections import OrderedDict
from operator import itemgetter
from typing import Mapping, Dict

from plenum.common.types import f

from plenum.common.constants import OP_FIELD_NAME, SCHEMA_IS_STRICT
from plenum.common.exceptions import MissingProtocolVersionError
from plenum.common.messages.fields import FieldValidator


class MessageValidator(FieldValidator):
    # the schema has to be an ordered iterable because the message class
    # can be create with positional arguments __init__(*args)

    schema = ()
    optional = False
    schema_is_strict = SCHEMA_IS_STRICT

    def __init__(self, schema_is_strict=SCHEMA_IS_STRICT):
        self.schema_is_strict = schema_is_strict

    def validate(self, dct):
        self._validate_fields_with_schema(dct, self.schema)
        self._validate_message(dct)

    def _validate_fields_with_schema(self, dct, schema):
        if not isinstance(dct, dict):
            self._raise_invalid_type(dct)
        schema_dct = dict(schema)
        required_fields = filter(lambda x: not x[1].optional, schema)
        required_field_names = map(lambda x: x[0], required_fields)
        missed_required_fields = set(required_field_names) - set(dct)
        if missed_required_fields:
            self._raise_missed_fields(*missed_required_fields)
        for k, v in dct.items():
            if k not in schema_dct:
                if self.schema_is_strict:
                    self._raise_unknown_fields(k, v)
            else:
                validation_error = schema_dct[k].validate(v)
                if validation_error:
                    self._raise_invalid_fields(k, v, validation_error)

    def _validate_message(self, dct):
        return None

    def _raise_invalid_type(self, dct):
        raise TypeError("{} invalid type {}, dict expected"
                        .format(self.__error_msg_prefix, type(dct)))

    def _raise_missed_fields(self, *fields):
        msg = "{} missed fields - {}. " \
            .format(self.__error_msg_prefix,
                    ', '.join(map(str, fields)))
        if any(field == f.PROTOCOL_VERSION.nm for field in map(str, fields)):
            raise MissingProtocolVersionError(msg)
        else:
            raise TypeError(msg)

    def _raise_unknown_fields(self, field, value):
        raise TypeError("{} unknown field - "
                        "{}={}".format(self.__error_msg_prefix,
                                       field, value))

    def _raise_invalid_fields(self, field, value, reason):
        raise TypeError("{} {} "
                        "({}={})".format(self.__error_msg_prefix, reason,
                                         field, value))

    def _raise_invalid_message(self, reason):
        raise TypeError("{} {}".format(self.__error_msg_prefix, reason))

    @property
    def __error_msg_prefix(self):
        return 'validation error [{}]:'.format(self.__class__.__name__)


class MessageBase(Mapping, MessageValidator):
    typename = None

    def __init__(self, *args, **kwargs):
        if args and kwargs:
            raise ValueError("*args, **kwargs cannot be used together")

        if kwargs:
            # op field is not required since there is self.typename
            kwargs.pop(OP_FIELD_NAME, None)

        argsLen = len(args or kwargs)
        if self.schema_is_strict and argsLen > len(self.schema):
            raise ValueError(
                "number of parameters {} should be less than or equal to "
                "the number of fields in schema {}"
                .format(argsLen, len(self.schema))
            )

        super().__init__()
        input_as_dict = kwargs if kwargs else self._join_with_schema(args)

        self.validate(input_as_dict)

        input_as_dict = self._post_process(input_as_dict)

        self._fields = OrderedDict(
            (name, input_as_dict[name])
            for name, _ in self.schema
            if name in input_as_dict)

    def _join_with_schema(self, args):
        return dict(zip(map(itemgetter(0), self.schema), args))

    def _post_process(self, input_as_dict: Dict) -> Dict:
        return input_as_dict

    def __getattr__(self, item):
        if item in self._fields:
            return self._fields[item]
        raise AttributeError(
            "'{}' object has no attribute '{}'"
            .format(self.__class__.__name__, item)
        )

    def __getitem__(self, key):
        values = list(self._fields.values())
        if isinstance(key, slice):
            return values[key]
        if isinstance(key, int):
            return values[key]
        raise TypeError("Invalid argument type.")

    def _asdict(self):
        return self.__dict__

    @property
    def __dict__(self):
        """
        Return a dictionary form.
        """
        m = self._fields.copy()
        m[OP_FIELD_NAME] = self.typename
        m.move_to_end(OP_FIELD_NAME, False)
        return m

    @property
    def __name__(self):
        return self.typename

    def __iter__(self):
        return self._fields.values().__iter__()

    def __len__(self):
        return len(self._fields)

    def items(self):
        return self._fields.items()

    def keys(self):
        return self._fields.keys()

    def values(self):
        return self._fields.values()

    def __str__(self):
        return "{}{}".format(self.typename, dict(self.items()))

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if not issubclass(other.__class__, self.__class__):
            return False
        return self._asdict() == other._asdict()

    def __hash__(self):
        h = 1
        for index, value in enumerate(list(self.__iter__())):
            h = h * (index + 1) * (hash(value) + 1)
        return h

    def __dir__(self):
        return self.keys()

    def __contains__(self, key):
        return key in self._fields

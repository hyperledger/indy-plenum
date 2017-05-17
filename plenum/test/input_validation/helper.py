from copy import deepcopy

import itertools


class TestCases:

    @property
    def positive_test_cases(self):
        raise NotImplementedError

    @property
    def negative_test_cases(self):
        raise NotImplementedError


class TestFieldBase(TestCases):

    def __init__(self, name=None):
        self.name = name

    @property
    def field_type(self):
        raise NotImplementedError


class PositiveNumberField(TestFieldBase):
    negative_test_cases = (-1,)
    positive_test_cases = (0, 1)
    field_type = int


class NonEmptyStringField(TestFieldBase):
    negative_test_cases = ('',)
    positive_test_cases = ('foo',)
    field_type = str


class HexString64Field(TestFieldBase):
    negative_test_cases = (
        '',
        'fba333c13994f63edd900cdc625b88d0dcee6dda7df2c6e9b5bcd5c1072c04f',  # 63 characters
        '77fba333c13994f63edd900cdc625b88d0dcee6dda7df2c6e9b5bcd5c1072c04f',  # 65 characters
        'xfba333c13994f63edd900cdc625b88d0dcee6dda7df2c6e9b5bcd5c1072c04f',  # first char is 'x'
    )
    positive_test_cases = (
        '7fba333c13994f63edd900cdc625b88d0dcee6dda7df2c6e9b5bcd5c1072c04f',  # lower case
        '7FBA333C13994F63EDD900CDC625B88D0DCEE6DDA7DF2C6E9B5BCD5C1072C04F'  # upper case
    )
    field_type = str


class TimestampField(TestFieldBase):
    negative_test_cases = (-1, 0)
    positive_test_cases = (1492619799822.973,)
    field_type = float


class ListField(TestFieldBase):
    field_type = list

    def __init__(self, name, inner_field):
        super().__init__(name)
        self.inner_field = inner_field

    @property
    def negative_test_cases(self):
        values = []
        for val in self.inner_field.negative_test_cases:
            values.append(list(self.inner_field.positive_test_cases) + [val])
        return values

    @property
    def positive_test_cases(self):
        return [self.inner_field.positive_test_cases]


class LedgerIdFiled(TestFieldBase):
    negative_test_cases = (-1, 2, 3)
    positive_test_cases = (0, 1)
    field_type = int


class IdrField(NonEmptyStringField):
    # TODO Only non empty string?
    pass


class RequestIdrField(TestFieldBase):
    field_type = list
    idr_field = IdrField()
    ts_field = TimestampField()

    @property
    def negative_test_cases(self):
        return [
            [[self.idr_field.positive_test_cases[0], self.ts_field.negative_test_cases[0]]],
            [[self.idr_field.negative_test_cases[0], self.ts_field.positive_test_cases[0]]],
        ]

    @property
    def positive_test_cases(self):
        return [
            [[self.idr_field.positive_test_cases[0], self.ts_field.positive_test_cases[0]]],
        ]


class IdentifierField(NonEmptyStringField):
    # TODO NonEmptyStringField definitely not enough
    pass


class NetworkPortField(TestFieldBase):
    field_type = int

    @property
    def negative_test_cases(self):
        return -1, 65535 + 1

    @property
    def positive_test_cases(self):
        return 0, 9700, 65535


class NetworkIpAddressField(TestFieldBase):
    field_type = str

    @property
    def negative_test_cases(self):
        return 'x', '0.0.0.0', '127.0.0', '256.0.0.1', 'x001:db8:85a3::8a2e:370:7334'

    @property
    def positive_test_cases(self):
        return '8.8.8.8', '127.0.0.1', '2001:db8:85a3::8a2e:370:7334'


class ServicesNodeOperation(TestFieldBase):
    field_type = list
    VALIDATOR = 'VALIDATOR'
    OBSERVER = 'OBSERVER'

    @property
    def negative_test_cases(self):
        return [
            ['foo'],
            [self.VALIDATOR, 'foo'],
        ]

    @property
    def positive_test_cases(self):
        return [
            [],
            [self.VALIDATOR],
            [self.VALIDATOR, self.OBSERVER],
            [self.VALIDATOR, self.VALIDATOR, self.OBSERVER],
        ]


class MessageDescriptor(TestFieldBase):
    field_type = dict

    def __init__(self, klass, fields, optional_fields=None, name=None):
        self.klass = klass
        self.fields = fields
        self.optional_fields = optional_fields
        super().__init__(name)

    @property
    def positive_test_cases(self):
        return list(itertools.chain(
            self.positive_test_cases_valid_message,
            self.positive_test_cases_missed_optional_field,
        ))

    @property
    def negative_test_cases(self):
        return list(itertools.chain(
            self.negative_test_cases_invalid_value,
            self.negative_test_cases_missed_required_field,
            self.negative_test_cases_extra_field,
            self.negative_test_cases_wrong_type,
        ))

    @property
    def positive_test_cases_valid_message(self):
        for field in self.fields:
            m = deepcopy(self._any_positive_case_copy)
            for v in field.positive_test_cases:
                m[field.name] = v
                yield m

    @property
    def positive_test_cases_missed_optional_field(self):
        for field in self.fields:
            if self.optional_fields and field in self.optional_fields:
                m = self._any_positive_case_copy
                del m[field.name]
                yield m

    @property
    def negative_test_cases_invalid_value(self):
        for field in self.fields:
            for val in field.negative_test_cases:
                m = self._any_positive_case_copy
                m[field.name] = val
                yield m

    @property
    def negative_test_cases_missed_required_field(self):
        for field in self.fields:
            if not self.optional_fields or field not in self.optional_fields:
                m = self._any_positive_case_copy
                del m[field.name]
                yield m

    @property
    def negative_test_cases_extra_field(self):
        m = self._any_positive_case_copy
        m.update(foo='bar')
        yield m

    @property
    def negative_test_cases_wrong_type(self):
        for field in self.fields:
            m = self._any_positive_case_copy
            for test_type in self._types_list:
                if test_type == field.field_type:
                    continue
                m[field.name] = test_type()
                yield m

    _types_list = (str, int, dict, list, float, bytes, bytearray)

    @property
    def _any_positive_case_copy(self):
        return deepcopy({field.name: field.positive_test_cases[0] for field in self.fields})

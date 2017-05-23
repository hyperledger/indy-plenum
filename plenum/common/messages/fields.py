import ipaddress
import re

from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID


class FieldValidator:

    def validate(self, val):
        raise NotImplementedError


class FieldBase(FieldValidator):
    _base_types = ()

    def __init__(self, optional=False):
        self.optional = optional

    def validate(self, val):
        type_er = self.__type_check(val)
        if type_er:
            return type_er
        return self._specific_validation(val)

    def _specific_validation(self, val):
        raise NotImplementedError

    def __type_check(self, val):
        if self._base_types is None:
            return  # type check is disabled
        for t in self._base_types:
            if isinstance(val, t):
                return
        return self._wrong_type_msg(val)

    def _wrong_type_msg(self, val):
        types_str = ', '.join(map(lambda x: x.__name__, self._base_types))
        return "expected types '{}', got '{}'" \
               "".format(types_str, type(val).__name__)


class NonEmptyStringField(FieldBase):
    _base_types = (str,)

    def _specific_validation(self, val):
        if not val:
            return 'empty string'


class SignatureField(FieldBase):
    _base_types = (str, type(None))
    # TODO do nothing because EmptySignature should be raised somehow

    def _specific_validation(self, val):
        return


class NonNegativeNumberField(FieldBase):

    _base_types = (int,)

    def _specific_validation(self, val):
        if val < 0:
            return 'negative value'


class ConstantField(FieldBase):
    _base_types = None

    def __init__(self, value, optional=False):
        super().__init__(optional=optional)
        self.value = value

    def _specific_validation(self, val):
        if val != self.value:
            return 'has to be equal {}'.format(self.value)


class IterableField(FieldBase):

    _base_types = (list, tuple)

    def __init__(self, inner_field_type: FieldValidator, optional=False):
        self.optional = optional
        self.inner_field_type = inner_field_type
        super().__init__(optional=optional)

    def _specific_validation(self, val):
        for v in val:
            check_er = self.inner_field_type.validate(v)
            if check_er:
                return check_er


class NetworkPortField(FieldBase):
    _base_types = (int,)

    def _specific_validation(self, val):
        if val < 0 or val > 65535:
            return 'network port out of the range 0-65535'


class NetworkIpAddressField(FieldBase):
    _base_types = (str,)
    _non_valid_addresses = ('0.0.0.0', '0:0:0:0:0:0:0:0', '::')

    def _specific_validation(self, val):
        invalid_address = False
        try:
            ipaddress.ip_address(val)
        except ValueError:
            invalid_address = True
        if invalid_address or val in self._non_valid_addresses:
            return 'invalid network ip address ({})'.format(val)


class ChooseField(FieldBase):
    _base_types = None

    def __init__(self, values, optional=None):
        self._possible_values = values
        super().__init__(optional=optional)

    def _specific_validation(self, val):
        if val not in self._possible_values:
            return "expected '{}' unknown value '{}'" \
                   "".format(', '.join(map(str, self._possible_values)), val)


class LedgerIdField(ChooseField):
    _base_types = (int,)
    ledger_ids = (POOL_LEDGER_ID, DOMAIN_LEDGER_ID)

    def __init__(self, optional=False):
        super().__init__(self.ledger_ids, optional=optional)


class IdentifierField(NonEmptyStringField):
    _base_types = (str, )
    # TODO implement the rules


class RequestIdentifierField(FieldBase):
    _base_types = (list, tuple)
    _length = 2

    def _specific_validation(self, val):
        if len(val) != self._length:
            return "should have length {}".format(self._length)
        idr_error = NonEmptyStringField().validate(val[0])
        if idr_error:
            return idr_error
        ts_error = TimestampField().validate(val[1])
        if ts_error:
            return ts_error


class TieAmongField(FieldBase):
    _base_types = (list, tuple)
    _length = 2
    # TODO eliminate duplication with RequestIdentifierField

    def _specific_validation(self, val):
        if len(val) != self._length:
            return "should have length {}".format(self._length)
        idr_error = NonEmptyStringField().validate(val[0])
        if idr_error:
            return idr_error
        ts_error = TimestampField().validate(val[1])
        if ts_error:
            return ts_error


class VerkeyField(NonEmptyStringField):
    _base_types = (str, )
    # TODO implement the rules


class HexField(FieldBase):
    _base_types = (str, )

    def __init__(self, length=None, optional=False):
        super().__init__(optional)
        self._length = length

    def _specific_validation(self, val):
        try:
            int(val, 16)
        except ValueError:
            return "invalid hex number '{}'".format(val)
        if self._length is not None and len(val) != self._length:
            return "length should be {} length".format(self._length)


class MerkleRootField(FieldBase):
    _base_types = (str, type(None))

    def _specific_validation(self, val):
        # TODO implement
        return None
    # def _specific_validation(self, val):
    #     if len(val) != 46:
    #         return 'length should be 46'


class TimestampField(FieldBase):
    _base_types = (float, int)

    def _specific_validation(self, val):
        # TODO finish implementation
        if val < 0:
            return 'should be a positive number'

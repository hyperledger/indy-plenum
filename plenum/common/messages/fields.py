import ipaddress
import json
import re
from abc import ABCMeta, abstractmethod
from typing import Iterable, Type

import base58
import dateutil

from common.exceptions import PlenumTypeError, PlenumValueError
from common.error import error
from common.version import (
    InvalidVersionError, VersionBase, GenericVersion
)
from crypto.bls.bls_multi_signature import MultiSignatureValue
from plenum import PLUGIN_LEDGER_IDS
from plenum.common.constants import VALID_LEDGER_IDS, CURRENT_PROTOCOL_VERSION
from plenum.common.plenum_protocol_version import PlenumProtocolVersion
from plenum.config import BLS_MULTI_SIG_LIMIT, DATETIME_LIMIT, VERSION_FIELD_LIMIT


class FieldValidator(metaclass=ABCMeta):
    """"
    Interface for field validators
    """
    # TODO INDY-2072 test optional
    def __init__(self, optional: bool = False):
        self.optional = optional

    @abstractmethod
    def validate(self, val):
        """
        Validates field value

        :param val: field value to validate
        :return: error message or None
        """


class FieldBase(FieldValidator, metaclass=ABCMeta):
    """
    Base class for field validators
    """

    _base_types = ()

    def __init__(self, optional=False, nullable=False):
        self.optional = optional
        self.nullable = nullable

    # TODO: `validate` should be renamed to `validation_error`
    def validate(self, val):
        """
        Performs basic validation of field value and then passes it for
        specific validation.

        :param val: field value to validate
        :return: error message or None
        """

        if self.nullable and val is None:
            return
        type_er = self.__type_check(val)
        if type_er:
            return type_er

        spec_err = self._specific_validation(val)
        if spec_err:
            return spec_err

    @abstractmethod
    def _specific_validation(self, val):
        """
        Performs specific validation of field. Should be implemented in
        subclasses. Use it instead of overriding 'validate'.

        :param val: field value to validate
        :return: error message or None
        """

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


# TODO: The fields below should be singleton.


class AnyField(FieldBase):
    _base_types = (object,)

    def _specific_validation(self, _):
        return


class BooleanField(FieldBase):
    _base_types = (bool,)

    def _specific_validation(self, val):
        return


class IntegerField(FieldBase):
    _base_types = (int,)

    def _specific_validation(self, val):
        return


class NonEmptyStringField(FieldBase):
    _base_types = (str,)

    def _specific_validation(self, val):
        if not val:
            return 'empty string'


class LimitedLengthStringField(FieldBase):
    _base_types = (str, )

    def __init__(self, max_length: int, can_be_empty=False, **kwargs):
        if not max_length > 0:
            raise PlenumValueError('max_length', max_length, '> 0')
        super().__init__(**kwargs)
        self._max_length = max_length
        self._can_be_empty = can_be_empty

    def _specific_validation(self, val):
        if not val and not self._can_be_empty:
            return 'empty string'
        if val and len(val) > self._max_length:
            val = val[:100] + ('...' if len(val) > 100 else '')
            return '{} is longer than {} symbols'.format(val, self._max_length)


class DatetimeStringField(FieldBase):
    _base_types = (str,)
    _exceptional_values = []

    def __init__(self, exceptional_values: Iterable[str]=[], **kwargs):
        super().__init__(**kwargs)
        if exceptional_values is not None:
            self._exceptional_values = exceptional_values

    def _specific_validation(self, val):
        if len(val) > DATETIME_LIMIT:
            val = val[:100] + ('...' if len(val) > 100 else '')
            return '{} is longer than {} symbols'.format(val, DATETIME_LIMIT)
        if val not in self._exceptional_values:
            try:
                dateutil.parser.parse(val)
            except Exception:
                return "datetime {} is not valid".format(val)


class FixedLengthField(FieldBase):
    _base_types = (str, )

    def __init__(self, length: int, **kwargs):
        if not isinstance(length, int):
            error('length should be integer', TypeError)
        if length < 1:
            error('should be greater than 0', ValueError)
        self.length = length
        super().__init__(**kwargs)

    def _specific_validation(self, val):
        if len(val) != self.length:
            return '{} should have length {}'.format(val, self.length)


class SignatureField(LimitedLengthStringField):
    _base_types = (str, type(None))

    def _specific_validation(self, val):
        if val is None:
            # TODO do nothing because EmptySignature should be raised somehow
            return
        if len(val) == 0:
            return "signature can not be empty"
        return super()._specific_validation(val)


class RoleField(FieldBase):
    _base_types = (str, type(None))

    # TODO implement

    def _specific_validation(self, val):
        return


class NonNegativeNumberField(FieldBase):

    _base_types = (int, )

    def _specific_validation(self, val):
        if val < 0:
            return 'negative value'


class ConstantField(FieldBase):
    _base_types = None

    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def _specific_validation(self, val):
        if val != self.value:
            return 'has to be equal {}'.format(self.value)


class IterableField(FieldBase):
    _base_types = (list, tuple)

    def __init__(self, inner_field_type: FieldValidator, min_length=None,
                 max_length=None, **kwargs):

        if not isinstance(inner_field_type, FieldValidator):
            raise PlenumTypeError(
                'inner_field_type', inner_field_type, FieldValidator)

        for k in ('min_length', 'max_length'):
            m = locals()[k]
            if m is not None:
                if not isinstance(m, int):
                    raise PlenumTypeError(k, m, int)
                if not m > 0:
                    raise PlenumValueError(k, m, '> 0')

        self.inner_field_type = inner_field_type
        self.min_length = min_length
        self.max_length = max_length
        super().__init__(**kwargs)

    def _specific_validation(self, val):
        if self.min_length is not None:
            if len(val) < self.min_length:
                return 'length should be at least {}'.format(self.min_length)
        if self.max_length is not None:
            if len(val) > self.max_length:
                return 'length should be at most {}'.format(self.max_length)

        for v in val:
            check_er = self.inner_field_type.validate(v)
            if check_er:
                return check_er


class MapField(FieldBase):
    _base_types = (dict,)

    def __init__(self, key_field: FieldBase,
                 value_field: FieldBase,
                 **kwargs):
        super().__init__(**kwargs)
        self.key_field = key_field
        self.value_field = value_field

    def _specific_validation(self, val):
        for k, v in val.items():
            key_error = self.key_field.validate(k)
            if key_error:
                return key_error
            val_error = self.value_field.validate(v)
            if val_error:
                return val_error


class AnyMapField(FieldBase):
    # A map where key and value can be of arbitrary types
    _base_types = (dict,)

    def _specific_validation(self, _):
        return


class NetworkPortField(FieldBase):
    _base_types = (int,)

    def _specific_validation(self, val):
        if val <= 0 or val > 65535:
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

    def __init__(self, values, **kwargs):
        self._possible_values = values
        super().__init__(**kwargs)

    def _specific_validation(self, val):
        if val not in self._possible_values:
            return "expected one of '{}', unknown value '{}'" \
                .format(', '.join(map(str, self._possible_values)), val)


class MessageField(FieldBase):
    _base_types = None

    def __init__(self, message_type, **kwargs):
        self._message_type = message_type
        super().__init__(**kwargs)

    def _specific_validation(self, val):
        if isinstance(val, self._message_type):
            return
        try:
            self._message_type(**val)
        except TypeError as ex:
            return "value {} cannot be represented as {} due to: {}" \
                .format(val, self._message_type.typename, ex)


class LedgerIdField(ChooseField):
    _base_types = (int,)
    ledger_ids = VALID_LEDGER_IDS

    def __init__(self, **kwargs):
        self.ledger_ids = self.update_with_plugin_ledger_ids()
        super().__init__(self.ledger_ids, **kwargs)

    @staticmethod
    def update_with_plugin_ledger_ids():
        return VALID_LEDGER_IDS + tuple(PLUGIN_LEDGER_IDS)


class Base58Field(FieldBase):
    _base_types = (str,)
    _alphabet = set(base58.alphabet.decode("utf-8"))

    def __init__(self, byte_lengths=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.byte_lengths = byte_lengths

    def _specific_validation(self, val):
        invalid_chars = set(val) - self._alphabet
        if invalid_chars:
            # only 10 chars to shorten the output
            # TODO: Why does it need to be sorted
            to_print = sorted(invalid_chars)[:10]
            return 'should not contain the following chars {}{}'.format(
                to_print, ' (truncated)' if len(to_print) < len(invalid_chars) else '')
        if self.byte_lengths is not None:
            # TODO could impact performance, need to check
            b58len = len(base58.b58decode(val))
            if b58len not in self.byte_lengths:
                return 'b58 decoded value length {} should be one of {}' \
                    .format(b58len, list(self.byte_lengths))


class IdentifierField(Base58Field):
    _base_types = (str,)

    def __init__(self, *args, **kwargs):
        # TODO the tests in client are failing because the field
        # can be short and long both. It is can be an error.
        # We have to double check the type of the field.
        super().__init__(byte_lengths=(16, 32), *args, **kwargs)


class DestNodeField(Base58Field):
    _base_types = (str,)

    def __init__(self, *args, **kwargs):
        # TODO the tests in client are failing because the field
        # can be short and long both. It is can be an error.
        # We have to double check the type of the field.
        super().__init__(byte_lengths=(16, 32), *args, **kwargs)


class DestNymField(Base58Field):
    _base_types = (str,)

    def __init__(self, *args, **kwargs):
        # TODO the tests in client are failing because the field
        # can be short and long both. It is can be an error.
        # We have to double check the type of the field.
        super().__init__(byte_lengths=(16, 32), *args, **kwargs)


class RequestIdentifierField(FieldBase):
    _base_types = (list, tuple)
    _length = 2
    _idr_field = IdentifierField()
    _rid_field = NonNegativeNumberField()

    def _specific_validation(self, val):
        if len(val) != self._length:
            return "should have length {}".format(self._length)
        # idr_error = IdentifierField().validate(val[0])
        if not isinstance(val[0], str):
            return 'identifier not present'
        from plenum.common.request import Request
        idr_error = any(self._idr_field.validate(i) for i in val[0].split(Request.idr_delimiter))
        if idr_error:
            return idr_error
        ts_error = self._rid_field.validate(val[1])
        if ts_error:
            return ts_error


class TieAmongField(FieldBase):
    _base_types = (list, tuple)
    _length = 2

    def __init__(self, max_length: int, **kwargs):
        super().__init__(**kwargs)
        self._max_length = max_length

    def _specific_validation(self, val):
        if len(val) != self._length:
            return "should have length {}".format(self._length)
        idr_error = LimitedLengthStringField(max_length=self._max_length).validate(val[0])
        if idr_error:
            return idr_error
        ts_error = NonNegativeNumberField().validate(val[1])
        if ts_error:
            return ts_error


class FullVerkeyField(FieldBase):
    _base_types = (str,)
    _validator = Base58Field(byte_lengths=(32,))

    def _specific_validation(self, val):
        # full base58
        return self._validator.validate(val)


class AbbreviatedVerkeyField(FieldBase):
    _base_types = (str,)
    _validator = Base58Field(byte_lengths=(16,))

    def _specific_validation(self, val):
        if not val.startswith('~'):
            return 'should start with a ~'
        # abbreviated base58
        return self._validator.validate(val[1:])


# TODO: think about making it a subclass of Base58Field
class VerkeyField(FieldBase):
    _base_types = (str,)
    _b58abbreviated = AbbreviatedVerkeyField()
    _b58full = FullVerkeyField()

    def _specific_validation(self, val):
        err_ab = self._b58abbreviated.validate(val)
        err_fl = self._b58full.validate(val)
        if err_ab and err_fl:
            return 'Neither a full verkey nor an abbreviated one. One of ' \
                   'these errors should be resolved:\n {}\n{}'.\
                format(err_ab, err_fl)


class HexField(FieldBase):
    _base_types = (str,)

    def __init__(self, length=None, **kwargs):
        super().__init__(**kwargs)
        self._length = length

    def _specific_validation(self, val):
        try:
            int(val, 16)
        except ValueError:
            return "invalid hex number '{}'".format(val)
        if self._length is not None and len(val) != self._length:
            return "length should be {} length".format(self._length)


class MerkleRootField(Base58Field):
    _base_types = (str,)

    def __init__(self, *args, **kwargs):
        super().__init__(byte_lengths=(32,), *args, **kwargs)


class TimestampField(FieldBase):
    _base_types = (int,)
    _oldest_time = 1499906902

    def _specific_validation(self, val):
        if val < self._oldest_time:
            return 'should be greater than {} but was {}'. \
                format(self._oldest_time, val)


class JsonField(LimitedLengthStringField):
    _base_types = (str,)

    def _specific_validation(self, val):
        # TODO: Need a mechanism to ensure a non-empty JSON if needed
        lim_str_err = super()._specific_validation(val)
        if lim_str_err:
            return lim_str_err
        try:
            json.loads(val)
        except json.decoder.JSONDecodeError:
            return 'should be a valid JSON string'


class SerializedValueField(FieldBase):
    _base_types = (bytes, str)

    def _specific_validation(self, val):
        if not val and not self.nullable:
            return 'empty serialized value'


class VersionField(LimitedLengthStringField):
    _base_types = (str,)

    def __init__(
            self,
            version_cls: Type[VersionBase] = GenericVersion,
            max_length: int = VERSION_FIELD_LIMIT,
            **kwargs
    ):
        super().__init__(max_length=max_length, **kwargs)
        self._version_cls = version_cls

    def _specific_validation(self, val):
        lim_str_err = super()._specific_validation(val)
        if lim_str_err:
            return lim_str_err

        try:
            self._version_cls(val)
        except InvalidVersionError as exc:
            return str(exc)
        else:
            return None


class TxnSeqNoField(FieldBase):
    _base_types = (int,)

    def _specific_validation(self, val):
        if val < 1:
            return 'cannot be smaller than 1'


class Sha256HexField(FieldBase):
    """
    Validates a sha-256 hash specified in hex
    """
    _base_types = (str,)
    regex = re.compile('^[A-Fa-f0-9]{64}$')

    def _specific_validation(self, val):
        if self.regex.match(val) is None:
            return 'not a valid hash (needs to be in hex too)'


class AnyValueField(FieldBase):
    """
    Stub field validator
    """
    _base_types = None

    def _specific_validation(self, val):
        pass


class StringifiedNonNegativeNumberField(NonNegativeNumberField):
    """
    This validator is needed because of json limitations: in some cases
    numbers being converted to strings.
    """
    # TODO: Probably this should be solved another way

    _base_types = (str, int)
    _num_validator = NonNegativeNumberField()

    def _specific_validation(self, val):
        try:
            return self._num_validator.validate(int(val))
        except ValueError:
            return "stringified int expected, but was '{}'" \
                .format(val)


class LedgerInfoField(FieldBase):
    _base_types = (list, tuple)
    _ledger_id_class = LedgerIdField

    def _specific_validation(self, val):
        if len(val) != 3:
            return 'should have size of 3'

        ledgerId, ledgerLength, merkleRoot = val
        # TODO test that as well
        for validator, value in ((self._ledger_id_class().validate, ledgerId),
                                 (NonNegativeNumberField().validate, ledgerLength),
                                 (MerkleRootField().validate, merkleRoot)):
            err = validator(value)
            if err:
                return err


class BlsMultiSignatureValueField(FieldBase):
    _base_types = (list, tuple)
    _ledger_id_class = LedgerIdField
    _state_root_hash_validator = MerkleRootField()
    _pool_state_root_hash_validator = MerkleRootField()
    _txn_root_hash_validator = MerkleRootField()
    _timestamp_validator = TimestampField()

    def _specific_validation(self, val):
        multi_sig_value = MultiSignatureValue(*val)

        err = self._ledger_id_class().validate(
            multi_sig_value.ledger_id)
        if err:
            return err

        err = self._state_root_hash_validator.validate(
            multi_sig_value.state_root_hash)
        if err:
            return err

        err = self._pool_state_root_hash_validator.validate(
            multi_sig_value.pool_state_root_hash)
        if err:
            return err

        err = self._txn_root_hash_validator.validate(
            multi_sig_value.txn_root_hash)
        if err:
            return err

        err = self._timestamp_validator.validate(
            multi_sig_value.timestamp)
        if err:
            return err


class BlsMultiSignatureField(FieldBase):
    _base_types = (list, tuple)
    _multisig_value_validator = BlsMultiSignatureValueField()
    _participants_validator = IterableField(NonEmptyStringField())
    _multisig_validator = \
        LimitedLengthStringField(max_length=BLS_MULTI_SIG_LIMIT)

    def _specific_validation(self, val):
        sig, participants, multi_sig_value = val

        err = self._multisig_value_validator.validate(multi_sig_value)
        if err:
            return err

        err = self._multisig_validator.validate(sig)
        if err:
            return err

        err = self._participants_validator.validate(participants)
        if err:
            return err
        if len(participants) == 0:
            return "multi-signature participants list is empty"


class ProtocolVersionField(FieldBase):
    _base_types = (int, type(None))

    def _specific_validation(self, val):
        if not PlenumProtocolVersion.has_value(val):
            return 'Unknown protocol version value. ' \
                   'Make sure that the latest LibIndy is used ' \
                   'and `set_protocol_version({})` is called' \
                .format(CURRENT_PROTOCOL_VERSION)
        if val != CURRENT_PROTOCOL_VERSION:
            return 'Message version ({}) differs from current protocol version. ' \
                   'Make sure that the latest LibIndy is used ' \
                   'and `set_protocol_version({})` is called' \
                .format(val, CURRENT_PROTOCOL_VERSION)

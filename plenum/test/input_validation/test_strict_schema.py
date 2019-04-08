import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, TXN_TYPE, TARGET_NYM, VERKEY, NYM
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.messages.fields import NonNegativeNumberField
from plenum.common.messages.message_base import MessageBase
from plenum.common.request import SafeRequest
from plenum.common.types import f, OPERATION
from plenum.test.input_validation.constants import TEST_TARGET_NYM, TEST_VERKEY_ABBREVIATED


class MessageTest(MessageBase):
    typename = 'MessageTest'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonNegativeNumberField()),
    )


class StrictMessageTest(MessageBase):
    typename = 'MessageTest'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonNegativeNumberField()),
    )
    schema_is_strict = True


def test_not_strict_by_default_args():
    msg = MessageTest(1, 2, 3)
    assert msg
    assert msg.a == 1
    assert msg.b == 2


def test_not_strict_by_default_kwargs():
    kwargs = {'a': 1, 'b': 2, 'c': 3}
    msg = MessageTest(**kwargs)
    assert msg
    assert msg.a == 1
    assert msg.b == 2


def test_strict_with_invalid_args_number():
    with pytest.raises(ValueError) as excinfo:
        StrictMessageTest(1, 2, 3)
    assert ("should be less than or equal to the number of fields "
            "in schema {}".format(len(MessageTest.schema))) in str(excinfo.value)


def test_strict_with_invalid_kwargs_number():
    kwargs = {'a': 1, 'b': 2, 'c': 3}
    with pytest.raises(ValueError) as excinfo:
        StrictMessageTest(**kwargs)
    assert ("should be less than or equal to the number of fields "
            "in schema {}".format(len(MessageTest.schema))) in str(excinfo.value)


def test_client_req_not_strict_by_default():
    validator = ClientMessageValidator(operation_schema_is_strict=False)
    operation = {
        TXN_TYPE: NYM,
        TARGET_NYM: TEST_TARGET_NYM,
        VERKEY: TEST_VERKEY_ABBREVIATED,
        "some_new_field_op1": "some_new_value_op1",
        "some_new_field_op2": "some_new_value_op2"
    }
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.SIG.nm: "signature",
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
                "some_new_field1": "some_new_value1",
                "some_new_field2": "some_new_value2"}
    validator.validate(req_dict)


def test_client_req_strict_operation():
    validator = ClientMessageValidator(operation_schema_is_strict=True)
    operation = {
        TXN_TYPE: NYM,
        TARGET_NYM: TEST_TARGET_NYM,
        VERKEY: TEST_VERKEY_ABBREVIATED,
        "some_new_field_op": "some_new_value_op"
    }
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.SIG.nm: "signature",
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
                "some_new_field": "some_new_value"}

    with pytest.raises(TypeError) as excinfo:
        validator.validate(req_dict)
    assert ("unknown field - some_new_field_op=some_new_value_op" in str(excinfo.value))


def test_client_req_strict():
    validator = ClientMessageValidator(operation_schema_is_strict=True,
                                       schema_is_strict=True)
    operation = {
        TXN_TYPE: NYM,
        TARGET_NYM: TEST_TARGET_NYM,
        VERKEY: TEST_VERKEY_ABBREVIATED,
    }
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.SIG.nm: "signature",
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
                "some_new_field": "some_new_value"}

    with pytest.raises(TypeError) as excinfo:
        validator.validate(req_dict)
    assert ("unknown field - some_new_field=some_new_value" in str(excinfo.value))


def test_client_safe_req_not_strict_by_default():
    operation = {
        TXN_TYPE: NYM,
        TARGET_NYM: TEST_TARGET_NYM,
        VERKEY: TEST_VERKEY_ABBREVIATED,
        "some_new_field_op1": "some_new_value_op1",
        "some_new_field_op2": "some_new_value_op2"
    }
    kwargs = {f.IDENTIFIER.nm: "1" * 16,
              f.REQ_ID.nm: 1,
              OPERATION: operation,
              f.SIG.nm: "signature",
              f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
              "some_new_field1": "some_new_value1",
              "some_new_field2": "some_new_value2"}
    assert SafeRequest(**kwargs) is not None

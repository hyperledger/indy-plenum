import pytest
from plenum.common.constants import TXN_TYPE, NYM, TARGET_NYM, \
    VERKEY
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.request import Request
from plenum.test.input_validation.constants import TEST_TARGET_NYM
from plenum.test.input_validation.constants import TEST_VERKEY_ABBREVIATED


@pytest.fixture()
def validator():
    return ClientMessageValidator(operation_schema_is_strict=True)


@pytest.fixture()
def operation():
    return {
        TXN_TYPE: NYM,
        TARGET_NYM: TEST_TARGET_NYM,
        VERKEY: TEST_VERKEY_ABBREVIATED
    }


@pytest.fixture()
def operation_invalid():
    return {
        TXN_TYPE: NYM,
        TARGET_NYM: "1",
        VERKEY: TEST_VERKEY_ABBREVIATED
    }


def test_minimal_valid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation).as_dict
    validator.validate(req_dict)


def test_with_signature_valid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       signature="signature").as_dict
    validator.validate(req_dict)


def test_with_digest_valid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation).as_dict
    req_dict["digest"] = "digest"
    validator.validate(req_dict)


def test_with_version_valid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       protocolVersion=1).as_dict
    validator.validate(req_dict)


def test_all_valid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       signature="signature",
                       protocolVersion=1).as_dict
    req_dict["digest"] = "digest"
    validator.validate(req_dict)


def test_signature_version_valid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       signature="signature",
                       protocolVersion=1).as_dict
    validator.validate(req_dict)


def test_all_identifier_invalid(validator, operation):
    req_dict = Request(identifier="1" * 5,
                       reqId=1,
                       operation=operation,
                       signature="signature",
                       protocolVersion=1).as_dict
    req_dict["digest"] = "digest"

    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match(r'b58 decoded value length 5 should be one of \[16, 32\]')


def test_all_reqid_invalid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=-500,
                       operation=operation,
                       signature="signature",
                       protocolVersion=1).as_dict
    req_dict["digest"] = "digest"

    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('negative value')


def test_all_operation_invalid(validator, operation_invalid):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation_invalid,
                       signature="signature",
                       protocolVersion=1).as_dict
    req_dict["digest"] = "digest"

    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match(r'\[ClientNYMOperation\]: b58 decoded value length 1 should be one of \[16, 32\]')


def test_less_than_minimal_valid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1).as_dict
    del req_dict['operation']
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('missed fields - operation')

    req_dict = Request(identifier="1" * 16,
                       operation=operation).as_dict
    del req_dict['reqId']
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('missed fields - reqId')

    req_dict = Request(reqId=1,
                       operation=operation).as_dict
    del req_dict['identifier']
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('missed fields - identifier')


def test_all_signature_invalid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       signature="",
                       protocolVersion=1).as_dict
    req_dict["digest"] = "digest"
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match("signature can not be empty")


def test_all_digest_invalid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       signature="signature",
                       protocolVersion=1).as_dict
    req_dict["digest"] = ""
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('empty string \(digest=\)')


def test_all_version_invalid(validator, operation):
    req_dict = Request(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       signature="signature",
                       protocolVersion=-5).as_dict
    req_dict["digest"] = "digest"
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('Unknown protocol version value -5')

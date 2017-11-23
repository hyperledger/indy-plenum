import pytest

from plenum.common.constants import TXN_TYPE, NYM, TARGET_NYM, \
    VERKEY
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.types import f, OPERATION
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


@pytest.fixture()
def request_dict(operation):
    return {f.IDENTIFIER.nm: "1" * 16,
            f.REQ_ID.nm: 1,
            OPERATION: operation,
            f.SIG.nm: "signature",
            f.DIGEST.nm: "digest",
            f.PROTOCOL_VERSION.nm: 1}


def test_minimal_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation}
    validator.validate(req_dict)


def test_with_signature_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.SIG.nm: "signature"}
    validator.validate(req_dict)


def test_with_digest_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.DIGEST.nm: "digest"}
    validator.validate(req_dict)


def test_with_version_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.PROTOCOL_VERSION.nm: 1}
    validator.validate(req_dict)


def test_all_valid(validator, request_dict):
    validator.validate(request_dict)


def test_signature_version_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.SIG.nm: "signature",
                f.PROTOCOL_VERSION.nm: 1}
    validator.validate(req_dict)


def test_all_identifier_invalid(validator, request_dict):
    request_dict[f.IDENTIFIER.nm] = "1" * 5
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_dict)
    ex_info.match(r'b58 decoded value length 5 should be one of \[16, 32\]')


def test_all_reqid_invalid(validator, request_dict):
    request_dict[f.REQ_ID.nm] = -500
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_dict)
    ex_info.match('negative value')


def test_all_operation_invalid(validator, operation_invalid, request_dict):
    request_dict[OPERATION] = operation_invalid
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_dict)
    ex_info.match(r'\[ClientNYMOperation\]: b58 decoded value length 1 should be one of \[16, 32\]')


def test_less_than_minimal_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1}
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('missed fields - operation')

    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                OPERATION: operation}
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('missed fields - reqId')

    req_dict = {f.REQ_ID.nm: 1,
                OPERATION: operation}
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('Missing both signatures and identifier')


def test_all_signature_invalid(validator, request_dict):
    request_dict[f.SIG.nm] = ""
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_dict)
    ex_info.match("signature can not be empty")


def test_all_digest_invalid(validator, request_dict):
    request_dict[f.DIGEST.nm] = ""
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_dict)
    ex_info.match('empty string \(digest=\)')


def test_all_version_invalid(validator, request_dict):
    request_dict[f.PROTOCOL_VERSION.nm] = -5
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_dict)
    ex_info.match('Unknown protocol version value -5')


def test_no_sigs(validator, operation):
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.SIG.nm: "signature",
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: 1
    }
    validator.validate(request_data)


def test_no_idr(validator, operation):
    request_data = {
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.SIGS.nm: {
            "1" * 16: "signature"
        },
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: 1
    }
    validator.validate(request_data)


def test_no_sigs_and_idr(validator, operation):
    request_data = {
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.SIG.nm: "signature",
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: 1
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Missing both signatures and identifier')

    request_data = {
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: 1
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Missing both signatures and identifier')

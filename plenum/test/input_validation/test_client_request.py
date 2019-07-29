import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.types import f, OPERATION
from plenum.test.input_validation.constants import TEST_IDENTIFIER_SHORT_2, TEST_IDENTIFIER_SHORT


@pytest.fixture(params=['operation_schema_is_strict', 'operation_schema_is_not_strict',
                        'schema_is_strict', 'schema_is_not_strict'])
def validator(request):
    operation_schema_is_strict = request.param == 'operation_schema_is_strict'
    schema_is_strict = request.param == 'schema_is_strict'
    return ClientMessageValidator(operation_schema_is_strict=operation_schema_is_strict,
                                  schema_is_strict=schema_is_strict)


@pytest.fixture(params=['sig', 'sigs'])
def request_dict(request, operation, taa_acceptance):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.DIGEST.nm: "digest",
                f.TAA_ACCEPTANCE.nm: taa_acceptance,
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
    if request.param == 'sig':
        req_dict[f.SIG.nm] = "signature"
    else:
        req_dict[f.SIGS.nm] = {req_dict[f.IDENTIFIER.nm]: "sig1",
                               TEST_IDENTIFIER_SHORT: "sig2",
                               TEST_IDENTIFIER_SHORT_2: "sig3"}
    return req_dict


def test_minimal_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
    validator.validate(req_dict)


def test_with_signature_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.SIG.nm: "signature",
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
    validator.validate(req_dict)


def test_with_digest_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.DIGEST.nm: "digest",
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
    validator.validate(req_dict)


def test_with_taa_acceptance_valid(validator, operation, taa_acceptance):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.TAA_ACCEPTANCE.nm: taa_acceptance,
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
    validator.validate(req_dict)


def test_with_version_valid(validator, operation):
    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
    validator.validate(req_dict)


def test_all_sig_valid(validator, request_dict):
    validator.validate(request_dict)


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
                f.REQ_ID.nm: 1,
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('missed fields - operation')

    req_dict = {f.IDENTIFIER.nm: "1" * 16,
                OPERATION: operation,
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
    with pytest.raises(TypeError) as ex_info:
        validator.validate(req_dict)
    ex_info.match('missed fields - reqId')

    req_dict = {f.REQ_ID.nm: 1,
                OPERATION: operation,
                f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION}
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


def test_all_taa_acceptance_invalid(
        validator, request_dict, taa_acceptance_invalid):
    request_dict[f.TAA_ACCEPTANCE.nm] = taa_acceptance_invalid
    with pytest.raises(
        TypeError, match=("should be greater than")
    ):
        validator.validate(request_dict)


def test_all_version_invalid(validator, request_dict):
    request_dict[f.PROTOCOL_VERSION.nm] = -5
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_dict)
    ex_info.match('Unknown protocol version value')


def test_no_idr_no_sig_with_sigs(validator, operation):
    request_data = {
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.SIGS.nm: {
            "1" * 16: "signature"
        },
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    validator.validate(request_data)


def test_no_idr_no_sigs_with_sig(validator, operation):
    request_data = {
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.SIG.nm: "signature",
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Missing both signatures and identifier')


def test_no_idr_no_sigs_no_sig(validator, operation):
    request_data = {
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Missing both signatures and identifier')


def test_idr_no_sigs_no_sig(validator, operation):
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    validator.validate(request_data)


def test_idr_not_in_signatures(validator, operation):
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIGS.nm: {TEST_IDENTIFIER_SHORT: "sig2"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('The identifier is not contained in signatures')

    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIGS.nm: {"1" * 16: "sig2"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    validator.validate(request_data)


def test_sig_and_sigs(validator, operation):
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIG.nm: "sig1",
        f.SIGS.nm: {"1" * 16: "sig1"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Request can not contain both fields "signatures" and "signature"')

    request_data = {
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIG.nm: "sig1",
        f.SIGS.nm: {TEST_IDENTIFIER_SHORT: "sig1"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Request can not contain both fields "signatures" and "signature"')


def test_endorser_must_be_in_sigs(validator, operation):
    # signature but no signatures
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.SIG.nm: "sig1",
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
        f.ENDORSER.nm: TEST_IDENTIFIER_SHORT
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Endorser must sign the request')

    # no signature and no signatures
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
        f.ENDORSER.nm: TEST_IDENTIFIER_SHORT
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Endorser must sign the request')

    # signatures without Endorser's DID
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIGS.nm: {"1" * 16: "sig1"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
        f.ENDORSER.nm: TEST_IDENTIFIER_SHORT
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Endorser must sign the request')


def test_idr_must_be_in_sigs_if_endorser(validator, operation):
    # signatures without Author's DID
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIGS.nm: {TEST_IDENTIFIER_SHORT: "sig1"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
        f.ENDORSER.nm: TEST_IDENTIFIER_SHORT
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match("Author must sign the request when sending via Endorser")

    # signatures without Author's DID but has signature
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIG.nm: "sig1",
        f.SIGS.nm: {TEST_IDENTIFIER_SHORT: "sig1"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
        f.ENDORSER.nm: TEST_IDENTIFIER_SHORT
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match('Request can not contain both fields "signatures" and "signature"')

    # no Identifier at all
    request_data = {
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIGS.nm: {TEST_IDENTIFIER_SHORT: "sig1"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
        f.ENDORSER.nm: TEST_IDENTIFIER_SHORT
    }
    with pytest.raises(TypeError) as ex_info:
        validator.validate(request_data)
    ex_info.match("Author's Identifier must be present when sending via Endorser")


def test_endorser_valid(validator, operation):
    request_data = {
        f.IDENTIFIER.nm: "1" * 16,
        f.REQ_ID.nm: 1,
        OPERATION: operation,
        f.DIGEST.nm: "digest",
        f.SIGS.nm: {TEST_IDENTIFIER_SHORT: "sig1",
                    "1" * 16: "sig2"},
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION,
        f.ENDORSER.nm: TEST_IDENTIFIER_SHORT
    }
    validator.validate(request_data)

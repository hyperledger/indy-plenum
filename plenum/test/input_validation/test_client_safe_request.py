import pytest

from plenum.common.constants import (
    CURRENT_PROTOCOL_VERSION, TXN_PAYLOAD_METADATA_TAA_ACCEPTANCE
)
from plenum.common.request import SafeRequest


@pytest.fixture
def kwargs_minimal(request, operation):
    return {
        'identifier': '1' * 16,
        'reqId': 1,
        'operation': operation,
        'protocolVersion': CURRENT_PROTOCOL_VERSION
    }


@pytest.fixture
def kwargs_all(kwargs_minimal, taa_acceptance):
    kwargs_minimal['signature'] = 'signature'
    kwargs_minimal[TXN_PAYLOAD_METADATA_TAA_ACCEPTANCE] = taa_acceptance
    return kwargs_minimal


def test_minimal_valid(kwargs_minimal):
    SafeRequest(**kwargs_minimal)


def test_with_signature_valid(kwargs_minimal):
    kwargs_minimal['signature'] = 'signature'
    SafeRequest(**kwargs_minimal)


# TODO suplicates test_minimal_valid
def test_with_version_valid(kwargs_minimal):
    SafeRequest(**kwargs_minimal)


def test_with_taa_acceptance_valid(kwargs_minimal, taa_acceptance):
    kwargs_minimal[TXN_PAYLOAD_METADATA_TAA_ACCEPTANCE] = taa_acceptance
    SafeRequest(**kwargs_minimal)


def test_all_valid(kwargs_all):
    SafeRequest(**kwargs_all)


def test_all_identifier_invalid(kwargs_all):
    kwargs_all['identifier'] = '1' * 5
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(**kwargs_all)
    ex_info.match(r'b58 decoded value length 5 should be one of \[16, 32\]')


def test_all_reqid_invalid(kwargs_all):
    kwargs_all['reqId'] = -500
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(**kwargs_all)
    ex_info.match('negative value')


def test_all_operation_invalid(kwargs_all, operation_invalid):
    kwargs_all['operation'] = operation_invalid
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(**kwargs_all)
    ex_info.match(r'\[ClientNYMOperation\]: b58 decoded value length 1 should be one of \[16, 32\]')


def test_less_than_minimal_valid(kwargs_minimal):
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(**{k: v for k, v in kwargs_minimal.items() if k != 'operation'})
    ex_info.match('missed fields - operation')

    with pytest.raises(TypeError) as ex_info:
        SafeRequest(**{k: v for k, v in kwargs_minimal.items() if k != 'reqId'})
    ex_info.match('missed fields - reqId')

    with pytest.raises(TypeError) as ex_info:
        SafeRequest(**{k: v for k, v in kwargs_minimal.items() if k != 'identifier'})
    ex_info.match('Missing both signatures and identifier')


def test_all_signature_invalid(kwargs_all):
    kwargs_all['signature'] = ''
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(**kwargs_all)
    ex_info.match("signature can not be empty")


def test_all_version_invalid(kwargs_all):
    kwargs_all['protocolVersion'] = -5
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(**kwargs_all)
    ex_info.match('Unknown protocol version value. '
                  'Make sure that the latest LibIndy is used')


def test_all_taa_acceptance_invalid(kwargs_all, taa_acceptance_invalid):
    kwargs_all[TXN_PAYLOAD_METADATA_TAA_ACCEPTANCE] = taa_acceptance_invalid
    with pytest.raises(
        TypeError, match=("should be greater than")
    ):
        SafeRequest(**kwargs_all)

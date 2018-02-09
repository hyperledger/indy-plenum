import pytest

from plenum.common.constants import TXN_TYPE, NYM, TARGET_NYM, \
    VERKEY
from plenum.common.request import SafeRequest
from plenum.test.input_validation.constants import TEST_TARGET_NYM
from plenum.test.input_validation.constants import TEST_VERKEY_ABBREVIATED


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


def test_minimal_valid(operation):
    assert SafeRequest(identifier="1" * 16,
                       reqId=1,
                       operation=operation)


def test_no_version_by_default(operation):
    req = SafeRequest(identifier="1" * 16,
                      reqId=1,
                      operation=operation)
    assert not req.protocolVersion


def test_with_signature_valid(operation):
    assert SafeRequest(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       signature="signature")


def test_with_version_valid(operation):
    assert SafeRequest(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       protocolVersion=1)


def test_no_version_valid(operation):
    safeReq = SafeRequest(identifier="1" * 16,
                          reqId=1,
                          operation=operation,
                          protocolVersion=None)
    assert safeReq
    assert not safeReq.protocolVersion


def test_all_valid(operation):
    assert SafeRequest(identifier="1" * 16,
                       reqId=1,
                       operation=operation,
                       signature="signature",
                       protocolVersion=1)


def test_all_identifier_invalid(operation):
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(identifier="1" * 5,
                    reqId=1,
                    operation=operation,
                    signature="signature",
                    protocolVersion=1)
    ex_info.match(r'b58 decoded value length 5 should be one of \[16, 32\]')


def test_all_reqid_invalid(operation):
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(identifier="1" * 16,
                    reqId=-500,
                    operation=operation,
                    signature="signature",
                    protocolVersion=1)
    ex_info.match('negative value')


def test_all_operation_invalid(operation_invalid):
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(identifier="1" * 16,
                    reqId=1,
                    operation=operation_invalid,
                    signature="signature",
                    protocolVersion=1)
    ex_info.match(r'\[ClientNYMOperation\]: b58 decoded value length 1 should be one of \[16, 32\]')


def test_less_than_minimal_valid(operation):
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(identifier="1" * 16,
                    reqId=1)
    ex_info.match('missed fields - operation')

    with pytest.raises(TypeError) as ex_info:
        SafeRequest(identifier="1" * 16,
                    operation=operation)
    ex_info.match('missed fields - reqId')

    with pytest.raises(TypeError) as ex_info:
        SafeRequest(reqId=1,
                    operation=operation)
    ex_info.match('Missing both signatures and identifier')


def test_all_signature_invalid(operation):
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(identifier="1" * 16,
                    reqId=1,
                    operation=operation,
                    signature="",
                    protocolVersion=1)
    ex_info.match("signature can not be empty")


def test_all_version_invalid(operation):
    with pytest.raises(TypeError) as ex_info:
        SafeRequest(identifier="1" * 16,
                    reqId=1,
                    operation=operation,
                    signature="signature",
                    protocolVersion=-5)
    ex_info.match('Unknown protocol version value -5')

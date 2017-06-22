import pytest

from plenum.test.input_validation.constants import \
        TEST_TARGET_NYM, TEST_VERKEY_SHORT, TEST_VERKEY_LONG, \
        TEST_B58_BY_DECODED_LEN 

from plenum.common.messages.client_request import ClientNYMOperation, \
    TXN_TYPE, NYM, TARGET_NYM, VERKEY

op_nym = ClientNYMOperation()

def test_odd_length_verkey_fails():
    with pytest.raises(TypeError) as ex_info:
        op_nym.validate({
            TXN_TYPE: NYM,
            TARGET_NYM: TEST_TARGET_NYM,
            VERKEY: 'F'*45
        })
    ex_info.match(r'b58 decoded value length 33 should be one of \[32\]')


def test_short_length_verkeys():
    assert not op_nym.validate({
        TXN_TYPE: NYM,
        TARGET_NYM: TEST_TARGET_NYM,
        VERKEY: TEST_VERKEY_SHORT
    })

    with pytest.raises(TypeError) as ex_info:
        op_nym.validate({
            TXN_TYPE: NYM,
            TARGET_NYM: TEST_TARGET_NYM,
            VERKEY: TEST_B58_BY_DECODED_LEN[16]
        })
    ex_info.match(r'b58 decoded value length 16 should be one of \[32\]')

    with pytest.raises(TypeError) as ex_info:
        op_nym.validate({
            TXN_TYPE: NYM,
            TARGET_NYM: TEST_TARGET_NYM,
            VERKEY: '~' + TEST_B58_BY_DECODED_LEN[32]
        })
    ex_info.match(r'b58 decoded value length 32 should be one of \[16\]')



def test_long_length_verkey_passes():
    assert not op_nym.validate({
        TXN_TYPE: NYM,
        TARGET_NYM: TEST_TARGET_NYM,
        VERKEY: TEST_VERKEY_LONG
    })

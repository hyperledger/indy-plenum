import pytest

from plenum.common.messages.client_request import ClientNYMOperation, \
    TXN_TYPE, NYM, TARGET_NYM, VERKEY

op_nym = ClientNYMOperation()

def test_odd_length_verkey_fails():
    with pytest.raises(TypeError) as ex_info:
        op_nym.validate({
            TXN_TYPE: NYM,
            TARGET_NYM: 'HebGWgHmicPtzr4BTHmSmXkDNL7CngDjYVcxqT5oprMw',
            VERKEY: 'F'*45
        })
    ex_info.match('b58 decoded value length 33 should be 32')

import pytest

from plenum.common.messages.client_request import ClientNYMOperation, \
    TXN_TYPE, NYM, TARGET_NYM, VERKEY

op_nym = ClientNYMOperation()

TETS_TARGET_NYM = 'HebGWgHmicPtzr4BTHmSmXkDNL7CngDjYVcxqT5oprMw'

def test_odd_length_verkey_fails():
    with pytest.raises(TypeError) as ex_info:
        op_nym.validate({
            TXN_TYPE: NYM,
            TARGET_NYM: TETS_TARGET_NYM,
            VERKEY: 'F'*45
        })
    ex_info.match(r'b58 decoded value length 33 should be one of \[32\]')


def test_short_length_verkey_passes():
    assert not op_nym.validate({
        TXN_TYPE: NYM,
        TARGET_NYM: TETS_TARGET_NYM,
        VERKEY: '~A43KHjJmjwFX71J1b5p61N'
    })


def test_long_length_verkey_passes():
    assert not op_nym.validate({
        TXN_TYPE: NYM,
        TARGET_NYM: TETS_TARGET_NYM,
        VERKEY: 'ED6cb1vtM8JEReLzUFnuGMZX4jt9jnEuDfwGTiAcARtD'
    })

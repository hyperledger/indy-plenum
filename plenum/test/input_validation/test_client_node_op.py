import pytest

from plenum.common.constants import (
    TXN_TYPE, TARGET_NYM, VERKEY, DATA, NODE, ALIAS
)
from plenum.common.messages.client_request import ClientNodeOperation

from plenum.test.input_validation.constants import (
    TEST_TARGET_NYM_LONG, TEST_VERKEY_ABBREVIATED
)

op = ClientNodeOperation()

def test_short_length_verkey_and_long_target_nym_failed():
    with pytest.raises(TypeError) as ex_info:
        op.validate({
            TXN_TYPE: NODE,
            TARGET_NYM: TEST_TARGET_NYM_LONG,
            VERKEY: TEST_VERKEY_ABBREVIATED,
            DATA: {
                ALIAS: 'aNode'
            }
        })
    ex_info.match(r'Abbreviated verkey cannot be combined with long target DID')


def test_ommitted_verkey_and_long_target_nym_passed():
    op.validate({
        TXN_TYPE: NODE,
        TARGET_NYM: TEST_TARGET_NYM_LONG,
        DATA: {
            ALIAS: 'aNode'
        }
    })

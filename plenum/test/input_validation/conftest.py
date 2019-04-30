import pytest
import hashlib

from plenum.common.constants import (
    TXN_TYPE, NYM, TARGET_NYM, VERKEY
)
from plenum.common.util import get_utc_epoch
from plenum.common.messages.fields import TimestampField
from plenum.common.types import f
from plenum.test.input_validation.constants import TEST_TARGET_NYM
from plenum.test.input_validation.constants import TEST_VERKEY_ABBREVIATED


@pytest.fixture
def operation():
    return {
        TXN_TYPE: NYM,
        TARGET_NYM: TEST_TARGET_NYM,
        VERKEY: TEST_VERKEY_ABBREVIATED
    }


@pytest.fixture
def operation_invalid():
    return {
        TXN_TYPE: NYM,
        TARGET_NYM: "1",
        VERKEY: TEST_VERKEY_ABBREVIATED
    }


@pytest.fixture
def taa():
    return {
        f.TAA_AML_TYPE.nm: 'some-aml',
        f.TAA_HASH.nm: hashlib.sha256(b'some-taa').hexdigest(),
        f.TAA_TIME.nm: get_utc_epoch(),
    }


@pytest.fixture
def taa_invalid(taa):
    taa[f.TAA_TIME.nm] = TimestampField._oldest_time - 1
    return taa

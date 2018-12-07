import pytest

from common.exceptions import PlenumValueError
from stp_core.crypto.util import cleanSeed


def test_cleanSeed():
    with pytest.raises(PlenumValueError):
        cleanSeed('1')
    cleanSeed('1' * 32)
    cleanSeed('1' * 64)

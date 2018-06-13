import pytest

import re

from common.exceptions import PlenumValueError, ValueUndefinedError
from plenum.common.config_helper import PConfigHelper, PNodeConfigHelper

def test_PConfigHelper_init():
    with pytest.raises(ValueUndefinedError):
        PConfigHelper(None)

    with pytest.raises(PlenumValueError) as excinfo:
        PConfigHelper({}, chroot=".")
    assert re.search(r"chroot.*expected: starts with '/'", str(excinfo.value))

def test_PNodeConfigHelper_init():
    with pytest.raises(ValueUndefinedError):
        PNodeConfigHelper(None, {})

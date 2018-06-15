import pytest

from stp_core.common.log import Logger


def test_apply_config():
    logger = Logger()
    with pytest.raises(ValueError):
        logger.apply_config(None)

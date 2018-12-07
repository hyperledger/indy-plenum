import pytest
import asyncio

from common.exceptions import PlenumValueError
from stp_core.loop.eventually import eventually


def test_api():
    loop = asyncio.get_event_loop()
    with pytest.raises(PlenumValueError):
        loop.run_until_complete(eventually(lambda x: True, timeout=0))
    with pytest.raises(PlenumValueError):
        loop.run_until_complete(eventually(lambda x: True, timeout=250))
    loop.close()

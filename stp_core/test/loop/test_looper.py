import pytest
import asyncio

from stp_core.loop.looper import Looper, Prodable


def test_hasProdable():
    looper = Looper(autoStart=False)
    with pytest.raises(ValueError):
        Looper().hasProdable(Prodable(), 'prodable')
    looper.shutdownSync()

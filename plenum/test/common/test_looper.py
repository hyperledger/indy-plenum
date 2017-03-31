import asyncio

from plenum.common.looper import Looper, Prodable
from plenum.common.startable import Status


class TestProdable(Prodable):
    def __init__(self, prods_per_activity=10, time_between_prod=.01):
        self._prods = 0
        self._prods_per_activity = prods_per_activity
        self._active_start = 0
        self._time_between_prod = time_between_prod

    def toggle_active(self):
        self._active_start = self._prods

    def _is_active(self):
        return self._prods - self._active_start < self._prods_per_activity

    def name(self):
        return self.__name__

    async def prod(self, limit) -> int:
        cur_prods = self._prods
        if self._is_active():
            self._prods += 1
            if self._time_between_prod:
                await asyncio.sleep(self._time_between_prod)
        return self._prods - cur_prods

    def start(self, loop):
        return

    def stop(self):
        return

    def get_status(self) -> Status:
        return None


def test_run_till_quiet():
    with Looper() as loop:
        p = TestProdable(prods_per_activity=10)
        p.toggle_active()
        loop.add(p)
        loop.run_till_quiet()
        assert p._prods == 10


def test_run_till_quiet_small():
    with Looper() as loop:
        p = TestProdable(prods_per_activity=1, time_between_prod=None)
        p.toggle_active()
        loop.add(p)
        loop.run_till_quiet(quiet_for=.001)
        assert p._prods == 1


def test_run_till_quiet_timeout():
    with Looper() as loop:
        p = TestProdable(prods_per_activity=100, time_between_prod=1)
        p.toggle_active()
        loop.add(p)
        loop.run_till_quiet(timeout=1)


def test_run_till_quiet_no_prodables():
    with Looper() as loop:
        loop.run_till_quiet()
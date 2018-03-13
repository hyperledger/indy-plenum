import time
from functools import partial

from stp_core.loop.looper import Looper
from plenum.common.motor import Motor
from plenum.server.has_action_queue import HasActionQueue


class Q1(Motor, HasActionQueue):
    def __init__(self, name):
        self.name = name
        self.results = {}
        Motor.__init__(self)
        HasActionQueue.__init__(self)

    def start(self, loop):
        pass

    async def prod(self, limit: int = None) -> int:
        return self._serviceActions()

    def meth(self, meth_name, x):
        if meth_name not in self.results:
            self.results[meth_name] = []
        self.results[meth_name].append((x, time.perf_counter()))


def test_action_scheduling():
    with Looper() as looper:
        q1 = Q1('q1')
        q1.meth1 = partial(q1.meth, 'meth1')

        looper.add(q1)
        q1._schedule(partial(q1.meth1, 1), 1)
        q1._schedule(partial(q1.meth1, 2), 2)

        looper.runFor(1.5)

        assert 'meth1' in q1.results
        assert 1 in [t[0] for t in q1.results['meth1']]
        assert 2 not in [t[0] for t in q1.results['meth1']]


def test_action_cancellation_by_id():
    with Looper() as looper:
        q1 = Q1('q1')
        q1.meth2 = partial(q1.meth, 'meth2')

        looper.add(q1)

        action = partial(q1.meth2, 1)
        aid = q1._schedule(action, 1)
        q1._schedule(action, 1.5)

        looper.runFor(0.5)

        q1._cancel(aid=aid)

        looper.runFor(1.5)

        assert 'meth2' in q1.results
        assert len(q1.results['meth2']) == 1


def test_action_cancellation_by_action():
    with Looper() as looper:
        q1 = Q1('q1')
        q1.meth2 = partial(q1.meth, 'meth2')
        q1.meth3 = partial(q1.meth, 'meth3')

        looper.add(q1)

        q1._schedule(partial(q1.meth2, 1), 1)

        action = partial(q1.meth3, 1)
        q1._schedule(action, 1)
        q1._schedule(action, 1.5)

        looper.runFor(0.5)

        q1._cancel(action=action)

        looper.runFor(1.5)

        assert 'meth2' in q1.results
        assert 'meth3' not in q1.results

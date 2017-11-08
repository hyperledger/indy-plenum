import time
from functools import partial

from stp_core.loop.looper import Looper
from plenum.common.motor import Motor
from plenum.server.has_action_queue import HasActionQueue


def testActionQueue():
    class Q1(Motor, HasActionQueue):
        def __init__(self, name):
            self.name = name
            self.results = {}
            Motor.__init__(self)
            HasActionQueue.__init__(self)

        def start(self, loop):
            pass

        async def prod(self, limit: int=None) -> int:
            return self._serviceActions()

        def meth(self, meth_name, x):
            if meth_name not in self.results:
                self.results[meth_name] = []
            self.results[meth_name].append((x, time.perf_counter()))

    with Looper() as looper:
        q1 = Q1('q1')

        q1.meth1 = partial(q1.meth, 'meth1')
        q1.meth2 = partial(q1.meth, 'meth2')
        q1.meth3 = partial(q1.meth, 'meth3')

        looper.add(q1)
        q1._schedule(partial(q1.meth1, 1), 2)
        q1._schedule(partial(q1.meth1, 2), 4)

        aid = q1._schedule(partial(q1.meth2, 1), 2)
        q1._schedule(partial(q1.meth2, 2), 4)
        q1._cancel(aid=aid)

        action = partial(q1.meth3, 1)
        q1._schedule(action, 4)
        q1._schedule(action, 6)

        looper.runFor(2.3)

        assert 'meth1' in q1.results
        assert 1 in [t[0] for t in q1.results['meth1']]
        assert 2 not in [t[0] for t in q1.results['meth1']]
        assert 'meth2' not in q1.results
        assert 'meth3' not in q1.results

        q1._cancel(action=action)

        looper.runFor(2.3)
        assert 2 in [t[0] for t in q1.results['meth1']]
        assert 'meth2' in q1.results
        assert 2 in [t[0] for t in q1.results['meth2']]
        assert 'meth3' not in q1.results

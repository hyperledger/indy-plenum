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

        async def prod(self, limit: int = None) -> int:
            return self._serviceActions()

        def meth1(self, x):
            if 'meth1' not in self.results:
                self.results['meth1'] = []
            self.results['meth1'].append((x, time.perf_counter()))

    with Looper(debug=True) as looper:
        q1 = Q1('q1')
        looper.add(q1)
        q1._schedule(partial(q1.meth1, 1), 2)
        q1._schedule(partial(q1.meth1, 2), 4)
        looper.runFor(2.3)
        assert 1 in [t[0] for t in q1.results['meth1']]
        assert 2 not in [t[0] for t in q1.results['meth1']]
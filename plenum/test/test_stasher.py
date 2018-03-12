import time
from collections import deque

import pytest

from plenum.test.stasher import Stasher


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def test_delay():
    q = deque([1,2,3])
    s = Stasher(q, "my-stasher")

    def delay_twos(item):
        if item == 2:
            return 2

    # Check that relevant items are stashed from deque
    s.delay(delay_twos)
    s.process()
    assert list(q) == [1, 3]

    # Pretend that we processed items that are not stashed
    q.clear()

    # Check that nothing happened after one second
    time.sleep(1)
    s.process()
    assert list(q) == []

    # Check that stashed items returned to deque after one more second
    time.sleep(1)
    s.process()
    assert list(q) == [2]

    # Check that items are no longer stashed when delays are reset
    s.resetDelays()
    s.process()
    assert list(q) == [2]

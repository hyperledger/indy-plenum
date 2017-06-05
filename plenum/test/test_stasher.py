import time
from collections import deque

import pytest

from plenum.test.stasher import Stasher


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def test_delay():
    x = deque()
    s = Stasher(x, "my-stasher")
    x.append(1)
    x.append(2)
    x.append(3)

    def delayTwos(item):
        if item == 2:
            return 2

    s.delay(delayTwos)

    s.process()
    r1 = x.popleft()
    assert r1 == 1

    r2 = x.popleft()
    assert r2 == 3

    with pytest.raises(IndexError):
        x.popleft()

    time.sleep(1)
    s.process()

    with pytest.raises(IndexError):
        x.popleft()

    time.sleep(1)
    s.process()

    r3 = x.popleft()
    assert r3 == 2

    x.append(2)
    s.resetDelays()
    s.process()
    assert 2 == x.popleft()

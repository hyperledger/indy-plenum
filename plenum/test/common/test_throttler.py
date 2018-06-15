import pytest
import time

from common.exceptions import PlenumTypeError, PlenumValueError
from stp_core.ratchet import Ratchet
from plenum.common.throttler import Throttler


def test_throttler_init_invalid_args():
    for windowSize in (None, '5', [4]):
        with pytest.raises(PlenumTypeError):
            Throttler(windowSize)

    for windowSize in (-1, 0):
        with pytest.raises(PlenumValueError):
            Throttler(windowSize)


def test_throttler_case1():
    """
    Tests throttler with default delay function
    """
    windowSize = 3
    throttler = Throttler(windowSize)
    testIterations = windowSize * 5
    for i in range(testIterations):
        hasAcquired, timeToWait = throttler.acquire()
        if i % windowSize == 0:
            assert hasAcquired
            assert round(timeToWait) == 0
        else:
            assert not hasAcquired
            assert windowSize - i % windowSize == round(timeToWait)
        time.sleep(1)


def test_throttler_case2():
    """
    Tests throttler with custom delay function
    """
    windowSize = 10
    testIterations = windowSize - 2
    ratchet = Ratchet(a=2, b=0.05, c=1, base=2, peak=windowSize)
    throttler = Throttler(windowSize, ratchet.get)
    cooldowns = [time.sleep(1) or throttler.acquire()[1]
                 for i in range(testIterations)]
    middle = len(cooldowns) // 2
    firstIteration, secondIteration = cooldowns[:middle], cooldowns[middle:]
    for a, b in zip(firstIteration, secondIteration):
        if not a == b == 0:
            assert b > a

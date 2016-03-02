import traceback

import pytest

from plenum.test.testing_utils import setupTestLogging

setupTestLogging()


def run(test, stopOnFail=True, maxTimes=None):
    count = 0
    passes = 0
    fails = 0
    while maxTimes is None or count < maxTimes:
        exitcode = pytest.main(test)
        count += 1
        if exitcode:
            fails += 1
            print("Test failed!")
            traceback.print_exc()
            if stopOnFail:
                break
        else:
            passes += 1
            print("Test passed.")

        print("current stats: successes: {} fails: {}".format(passes, fails))


run("monitoring/test_instance_change_with_Delta.py",
    stopOnFail=False, maxTimes=100)

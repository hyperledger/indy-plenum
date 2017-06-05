import os
import logging
import shutil
import time
from stp_core.common.logging.TimeAndSizeRotatingFileHandler \
    import TimeAndSizeRotatingFileHandler



def cleanFolder(path):
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return path


@pytest.mark.skip(reason="SOV-950")
def test_time_log_rotation():
    logDirPath = cleanFolder("/tmp/plenum/test_time_log_rotation")
    logFile = os.path.join(logDirPath, "log")
    logger = logging.getLogger('test_time_log_rotation-logger')

    logger.setLevel(logging.DEBUG)
    handler = TimeAndSizeRotatingFileHandler(logFile, interval=1, when='s')
    logger.addHandler(handler)
    for i in range(3):
        time.sleep(1)
        logger.debug("line")
    assert len(os.listdir(logDirPath)) == 4 # initial + 3 new


@pytest.mark.skip(reason="SOV-950")
def test_size_log_rotation():
    logDirPath = cleanFolder("/tmp/plenum/test_size_log_rotation")
    logFile = os.path.join(logDirPath, "log")
    logger = logging.getLogger('test_time_log_rotation-logger')

    logger.setLevel(logging.DEBUG)
    handler = TimeAndSizeRotatingFileHandler(
        logFile, maxBytes=(4 + len(os.linesep)) * 4 + 1)
    logger.addHandler(handler)
    for i in range(20):
        logger.debug("line")
    handler.flush()

    assert len(os.listdir(logDirPath)) == 5


@pytest.mark.skip(reason="SOV-950")
def test_time_and_size_log_rotation():
    logDirPath = cleanFolder("/tmp/plenum/test_time_and_size_log_rotation")
    logFile = os.path.join(logDirPath, "log")
    logger = logging.getLogger('test_time_and_size_log_rotation-logger')

    logger.setLevel(logging.DEBUG)
    handler = TimeAndSizeRotatingFileHandler(
        logFile, maxBytes=(4 + len(os.linesep)) * 4 + 1, interval=1, when="s")
    logger.addHandler(handler)

    for i in range(20):
        logger.debug("line")

    for i in range(3):
        time.sleep(1)
        logger.debug("line")

    assert len(os.listdir(logDirPath)) == 8

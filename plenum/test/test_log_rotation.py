import os
import logging
import shutil
import time
from stp_core.common.logging.TimeAndSizeRotatingFileHandler \
    import TimeAndSizeRotatingFileHandler


def test_time_log_rotation(tdir_for_func):
    logDirPath = tdir_for_func
    logFile = os.path.join(logDirPath, "log")
    logger = logging.getLogger('test_time_log_rotation-logger')

    logger.setLevel(logging.DEBUG)
    handler = TimeAndSizeRotatingFileHandler(logFile, interval=1, when='s')
    logger.addHandler(handler)
    for i in range(3):
        time.sleep(1)
        logger.debug("line")
    assert len(os.listdir(logDirPath)) == 4 # initial + 3 new


def test_size_log_rotation(tdir_for_func):
    logDirPath = tdir_for_func
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


def test_time_and_size_log_rotation(tdir_for_func):
    logDirPath = tdir_for_func
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


def test_time_and_size_log_rotation1(tdir_for_func):
    logDirPath = tdir_for_func
    logFile = os.path.join(logDirPath, "log")
    logger = logging.getLogger('test_time_and_size_log_rotation-logger')

    logger.setLevel(logging.DEBUG)
    record_count = 100
    record_text = 'line'
    record_length = len(record_text)+len(str(record_count))
    handler = TimeAndSizeRotatingFileHandler(
        logFile, maxBytes=(record_length + len(os.linesep)) * 4 + 1, interval=1,
        when="h", backupCount=10, utc=True)
    logger.addHandler(handler)

    for i in range(1, record_count+1):
        pad_length = record_length - (len(record_text)+len(str(i)))
        logger.debug('{}{}{}'.format(record_text, '0'*pad_length, str(i)))

    # Look at the contents of `logDirPath` to see the problem

    # for i in range(3):
    #     time.sleep(1)
    #     logger.debug("line")


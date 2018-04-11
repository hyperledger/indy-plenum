import os
import logging
import time
import collections

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
    assert len(os.listdir(logDirPath)) == 4  # initial + 3 new


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
    log_dir_path = tdir_for_func
    logFile = os.path.join(log_dir_path, "log")
    logger = logging.getLogger('test_time_and_size_log_rotation-logger1')

    logger.setLevel(logging.DEBUG)
    record_count = 50
    record_per_file = 4
    backup_count = 5
    cir_buffer = collections.deque(maxlen=(backup_count + 1) * record_per_file)
    record_text = 'line'
    record_length = len(record_text) + len(str(record_count))

    handler = TimeAndSizeRotatingFileHandler(
        logFile,
        maxBytes=(record_length + len(os.linesep)) * record_per_file + 1,
        interval=1, when="h", backupCount=backup_count, utc=True)
    logger.addHandler(handler)

    for i in range(1, record_count + 1):
        pad_length = record_length - (len(record_text) + len(str(i)))
        line = '{}{}{}'.format(record_text, '0' * pad_length, str(i))
        logger.debug(line)
        cir_buffer.append(line)
        if i % record_per_file == 0:
            # waiting since last modified time cannot offer good enough
            # precision
            time.sleep(.5)

    circ_buffer_set = set(cir_buffer)
    assert len(cir_buffer) == len(circ_buffer_set)
    assert len(os.listdir(log_dir_path)) == (backup_count + 1)
    for file_name in os.listdir(log_dir_path):
        with open(os.path.join(log_dir_path, file_name)) as file:
            for line in file.readlines():
                assert line.strip() in circ_buffer_set

import os
import logging
import collections
import pytest
import importlib

from stp_core.common.logging.CompressingFileHandler \
    import CompressingFileHandler
from stp_core.common.log import Logger


@pytest.fixture(params=[None, "gz", "xz"], ids=["uncompressed", "gzipped", "xzipped"])
def log_compression(request):
    return request.param


def test_default_log_rotation_config_is_correct(tdir_for_func):
    log_dir_path = tdir_for_func
    log_file = os.path.join(log_dir_path, "log")
    logger = Logger()

    # Assert this doesn't fail
    logger.enableFileLogging(log_file)


def test_dynamic_log_import_works_as_expected():
    # Assert that nothing here fails
    log_module = importlib.import_module("stp_core.common.log")
    logger = log_module.getlogger()
    logger.info("test")


def test_log_file_matcher_works_as_expected(tdir_for_func, log_compression):
    log_dir_path = tdir_for_func
    log_file = os.path.join(log_dir_path, "log")
    handler = CompressingFileHandler(log_file, delay=True, compression=log_compression)

    assert handler.log_pattern.match("log")
    assert handler.log_pattern.match("log.42")
    assert handler.log_pattern.match("log.42.gz")
    assert handler.log_pattern.match("log.42.xz")

    assert not handler.log_pattern.match("log.tmp")
    assert not handler.log_pattern.match("log.tmp.gz")
    assert not handler.log_pattern.match("log.42.tmp")
    assert not handler.log_pattern.match("log.tmp.42")

    assert not handler.log_pattern.match("tmp_log")
    assert not handler.log_pattern.match("tmp_log.42")
    assert not handler.log_pattern.match("tmp_log.42.gz")
    assert not handler.log_pattern.match("tmp_log.42.xz")


def test_log_rotation(tdir_for_func, log_compression):
    log_dir_path = tdir_for_func
    log_file = os.path.join(log_dir_path, "log")
    logger = logging.getLogger('test_log_rotation-logger')

    logger.setLevel(logging.DEBUG)
    handler = CompressingFileHandler(log_file,
                                     maxBytes=(4 + len(os.linesep)) * 4 + 1,
                                     compression=log_compression)
    logger.addHandler(handler)
    for i in range(20):
        logger.debug("line")
    handler.flush()
    handler._finish_compression()

    assert all(handler.log_pattern.match(name) for name in os.listdir(log_dir_path))
    assert len(os.listdir(log_dir_path)) == 5


def test_log_rotation1(tdir_for_func, log_compression):
    log_dir_path = tdir_for_func
    log_file = os.path.join(log_dir_path, "log")
    logger = logging.getLogger('test_log_rotation-logger1')

    logger.setLevel(logging.DEBUG)
    record_count = 50
    record_per_file = 4
    backup_count = 5
    cir_buffer = collections.deque(maxlen=(backup_count + 1) * record_per_file)
    record_text = 'line'
    record_length = len(record_text) + len(str(record_count))

    handler = CompressingFileHandler(log_file,
                                     maxBytes=(record_length + len(os.linesep)) * record_per_file + 1,
                                     backupCount=backup_count,
                                     compression=log_compression)
    logger.addHandler(handler)

    for i in range(1, record_count + 1):
        pad_length = record_length - (len(record_text) + len(str(i)))
        line = '{}{}{}'.format(record_text, '0' * pad_length, str(i))
        logger.debug(line)
        cir_buffer.append(line)

    handler._finish_compression()

    cir_buffer_set = set(cir_buffer)
    assert len(cir_buffer) == len(cir_buffer_set)
    assert all(handler.log_pattern.match(name) for name in os.listdir(log_dir_path))
    assert len(os.listdir(log_dir_path)) == (backup_count + 1)
    for file_name in os.listdir(log_dir_path):
        with CompressingFileHandler._open_log(os.path.join(log_dir_path, file_name), "rt") as file:
            for line in file.readlines():
                assert line.strip() in cir_buffer_set

import pytest
import re
import functools
import logging

from stp_core.common.logging.handlers import TestingHandler


@pytest.fixture(scope="function")
def logsearch(request):
    logHandlers = []

    def add(hdlr):
        nonlocal logHandlers
        logging.getLogger().addHandler(hdlr)
        logHandlers.append(hdlr)

    def cleanup(hdlr=None):
        nonlocal logHandlers
        if hdlr is None:
            for hdlr in logHandlers:
                logging.getLogger().removeHandler(hdlr)
            logHandlers.clear()
        else:
            try:
                logHandlers.remove(hdlr)
            except ValueError:
                pass
            else:
                logging.getLogger().removeHandler(hdlr)

    request.addfinalizer(cleanup)

    def wrapper(levels=None, files=None, funcs=None, msgs=None):
        found = []

        reMsgs = None if msgs is None else re.compile('|'.join(msgs))

        class TestingFilter(logging.Filter):
            def filter(self, record):
                return (
                    (levels is None or
                        record.levelname in levels) and
                    (files is None or
                        record.filename in files) and
                    (funcs is None or
                        record.funcName in funcs) and
                    (reMsgs is None or
                        reMsgs.search(record.getMessage()) is not None)
                )

        def tester(record):
            nonlocal found
            found.append(record)

        hdlr = TestingHandler(tester)
        hdlr.addFilter(TestingFilter())

        add(hdlr)

        return found, functools.partial(cleanup, hdlr)

    return wrapper

import logging
import sys

from ioflo.base.consoling import getConsole
from common.error import error
from stp_core.common.log import getlogger, TRACE_LOG_LEVEL

logger = getlogger()


def checkDblImp():
    """
    Added this because I spent the better part of an evening troubleshooting an
    issue cause by double import. We were importing test.helper in one
    place, and test_helper in another, and python sees them as two different
    modules, and imported the same file twice. This caused genHa to be loaded
    twice, which caused overlapping ports to be assigned. Took a long time to
    track this down. I'm sure there's a better way to do this, but this seems
    to work for the basic testing I did.
    """
    logger.info("-------------checking for double imports-------------")
    ignore = {'posixpath.py',
              'helpers/pydev/pydevd.py',
              'importlib/_bootstrap.py',
              'importlib/_bootstrap_external.py',
              'helpers/pycharm/pytestrunner.py',
              'test/__init__.py',
              'site-packages/pytest.py',
              'python3.5/os.py',
              'python3.5/re.py'}

    files = [x.__file__ for x in list(sys.modules.values())
             if hasattr(x, "__file__")]
    dups = set([x for x in files if files.count(x) > 1])
    ignoreddups = {d for d in dups for i in ignore if i in d}
    filtereddups = dups - ignoreddups
    if filtereddups:
        error("Doubly imported files detected {}".format(filtereddups))


def setupTestLogging():
    logging.basicConfig(
        level=TRACE_LOG_LEVEL,
        format='{relativeCreated:,.0f} {levelname:7s} {message:s}',
        style='{')
    console = getConsole()
    console.reinit(verbosity=console.Wordage.concise)


class FakeSomething:
    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            setattr(self, name, value)

import logging
import sys

from ioflo.base.consoling import getConsole

from zeno.common.util import error, addTraceToLogging, TRACE_LOG_LEVEL


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
    logging.info("-------------checking for double imports-------------")
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
    addTraceToLogging()

    logging.basicConfig(
            level=TRACE_LOG_LEVEL,
            format='{relativeCreated:,.0f} {levelname:7s} {message:s}',
            style='{')
    console = getConsole()
    console.reinit(verbosity=console.Wordage.terse)


class adict(dict):
    marker = object()

    def __init__(self, **kwargs):
        super().__init__()
        for key in kwargs:
            self.__setitem__(key, kwargs[key])

    def __setitem__(self, key, value):
        if isinstance(value, dict) and not isinstance(value, adict):
            value = adict(**value)
        super(adict, self).__setitem__(key, value)

    def __getitem__(self, key):
        found = self.get(key, adict.marker)
        if found is adict.marker:
            found = adict()
            super(adict, self).__setitem__(key, found)
        return found

    __setattr__ = __setitem__
    __getattr__ = __getitem__

import inspect
import logging
import os
import sys
from plenum.common.logging.TimeAndSizeRotatingFileHandler \
    import TimeAndSizeRotatingFileHandler

from ioflo.base.consoling import getConsole, Console

TRACE_LOG_LEVEL = 5
DISPLAY_LOG_LEVEL = 25


class CustomAdapter(logging.LoggerAdapter):
    def trace(self, msg, *args, **kwargs):
        self.log(TRACE_LOG_LEVEL, msg, *args, **kwargs)

    def display(self, msg, *args, **kwargs):
        self.log(DISPLAY_LOG_LEVEL, msg, *args, **kwargs)


class CliHandler(logging.Handler):
    def __init__(self, callback):
        """
        Initialize the handler.
        """
        super().__init__()
        self.callback = callback

    def emit(self, record):
        """
        Passes the log record back to the CLI for rendering
        """
        if hasattr(record, "cli"):
            if record.cli:
                self.callback(record, record.cli)
        elif record.levelno >= logging.INFO:
            self.callback(record)


class DemoHandler(logging.Handler):
    def __init__(self, callback):
        """
        Initialize the handler.
        """
        super().__init__()
        self.callback = callback

    def emit(self, record):
        if hasattr(record, "demo"):
            if record.cli:
                self.callback(record, record.cli)
        elif record.levelno >= logging.INFO:
            self.callback(record)


loggingConfigured = False


def getlogger(name=None):
    if not loggingConfigured:
        setupLogging(TRACE_LOG_LEVEL)
    if not name:
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        name = inspect.getmodule(calframe[1][0]).__name__
    logger = logging.getLogger(name)
    return logger


class TestingHandler(logging.Handler):
    def __init__(self, tester):
        """
        Initialize the handler.
        """
        super().__init__()
        self.tester = tester

    def emit(self, record):
        """
        Captures a record.
        """
        self.tester(record)


def setupLogging(log_level, raet_log_level=None, filename=None,
                 raet_log_file=None):
    """
    Setup for logging.
    log level is TRACE by default.
    """

    from plenum.common.config_util import getConfig
    # TODO: This should take directory
    config = getConfig()
    addTraceToLogging()
    addDisplayToLogging()

    logHandlers = []
    if filename:
        d = os.path.dirname(filename)
        if not os.path.exists(d):
            os.makedirs(d)
        fileHandler = TimeAndSizeRotatingFileHandler(
            filename,
            when=config.logRotationWhen,
            interval=config.logRotationInterval,
            backupCount=config.logRotationBackupCount,
            utc=True,
            maxBytes=config.logRotationMaxBytes)
        logHandlers.append(fileHandler)
    else:
        logHandlers.append(logging.StreamHandler(sys.stdout))

    fmt = logging.Formatter(fmt=config.logFormat, style=config.logFormatStyle)

    for h in logHandlers:
        if h.formatter is None:
            h.setFormatter(fmt)
        logging.root.addHandler(h)

    logging.root.setLevel(log_level)

    console = getConsole()

    defaultVerbosity = getRAETLogLevelFromConfig("RAETLogLevel",
                                                 Console.Wordage.terse, config)
    logging.info("Choosing RAET log level {}".format(defaultVerbosity),
                 extra={"cli": False})
    verbosity = raet_log_level \
        if raet_log_level is not None \
        else defaultVerbosity
    raetLogFilePath = raet_log_file or getRAETLogFilePath("RAETLogFilePath",
                                                          config)
    console.reinit(verbosity=verbosity, path=raetLogFilePath, flushy=True)
    global loggingConfigured
    loggingConfigured = True


def getRAETLogLevelFromConfig(paramName, defaultValue, config):
    try:
        defaultVerbosity = config.__getattribute__(paramName)
        defaultVerbosity = Console.Wordage.__getattribute__(defaultVerbosity)
    except AttributeError:
        defaultVerbosity = defaultValue
        logging.debug("Ignoring RAET log level {} from config and using {} "
                      "instead".format(paramName, defaultValue))
    return defaultVerbosity


def getRAETLogFilePath(paramName, config):
    try:
        filePath = config.__getattribute__(paramName)
    except AttributeError:
        filePath = None
    return filePath


def addTraceToLogging():
    logging.addLevelName(TRACE_LOG_LEVEL, "TRACE")

    def trace(self, message, *args, **kwargs):
        if self.isEnabledFor(TRACE_LOG_LEVEL):
            self._log(TRACE_LOG_LEVEL, message, args, **kwargs)

    logging.Logger.trace = trace


def addDisplayToLogging():
    logging.addLevelName(DISPLAY_LOG_LEVEL, "DISPLAY")

    def display(self, message, *args, **kwargs):
        if self.isEnabledFor(DISPLAY_LOG_LEVEL):
            self._log(DISPLAY_LOG_LEVEL, message, args, **kwargs)

    logging.Logger.display = display

import inspect
import logging
import os
import sys
from ioflo.base.consoling import getConsole, Console
from stp_core.common.logging.TimeAndSizeRotatingFileHandler import TimeAndSizeRotatingFileHandler
from stp_core.common.util import Singleton
from stp_core.common.logging.handlers import CliHandler
from stp_core.common.config.util import getConfig

TRACE_LOG_LEVEL = 5
DISPLAY_LOG_LEVEL = 25

# TODO: move it to plenum-utils


class CustomAdapter(logging.LoggerAdapter):
    def trace(self, msg, *args, **kwargs):
        self.log(TRACE_LOG_LEVEL, msg, *args, **kwargs)

    def display(self, msg, *args, **kwargs):
        self.log(DISPLAY_LOG_LEVEL, msg, *args, **kwargs)


def getlogger(name: object = None) -> logging.Logger:
    return Logger().getlogger(name)


class Logger(metaclass=Singleton):
    def __init__(self, config=None):

        # TODO: This should take directory
        self._config = config or getConfig()
        self._addTraceToLogging()
        self._addDisplayToLogging()
        self.apply_config(self._config)

    @staticmethod
    def getlogger(name=None):
        if not name:
            curframe = inspect.currentframe()
            calframe = inspect.getouterframes(curframe, 2)
            name = inspect.getmodule(calframe[1][0]).__name__
        logger = logging.getLogger(name)
        return logger

    @staticmethod
    def setLogLevel(log_level):
        logging.root.setLevel(log_level)

    def apply_config(self, config):
        assert config

        self._config = config
        self._handlers = {}
        self._clearAllHandlers()
        self._format = logging.Formatter(fmt=self._config.logFormat,
                                         style=self._config.logFormatStyle)

        if self._config.enableStdOutLogging:
            self.enableStdLogging()

        logLevel = logging.INFO
        if hasattr(self._config, "logLevel"):
            logLevel = self._config.logLevel
        self.setLogLevel(logLevel)

    def enableStdLogging(self):
        # only enable if CLI is not
        if 'cli' in self._handlers:
            raise RuntimeError('cannot configure STD logging '
                               'when CLI logging is enabled')
        new = logging.StreamHandler(sys.stdout)
        self._setHandler('std', new)

    def enableCliLogging(self, callback, override_tags=None):
        h = CliHandler(callback, override_tags)
        self._setHandler('cli', h)
        # assumption is there's never a need to have std logging when in CLI
        self._clearHandler('std')

    def enableFileLogging(self, filename):
        d = os.path.dirname(filename)
        if not os.path.exists(d):
            os.makedirs(d)
        new = TimeAndSizeRotatingFileHandler(
            filename,
            when=self._config.logRotationWhen,
            interval=self._config.logRotationInterval,
            backupCount=self._config.logRotationBackupCount,
            utc=True,
            maxBytes=self._config.logRotationMaxBytes)
        self._setHandler('file', new)

    def _setHandler(self, typ: str, new_handler):
        if new_handler.formatter is None:
            new_handler.setFormatter(self._format)

        # assuming indempotence and removing old one first
        self._clearHandler(typ)

        self._handlers[typ] = new_handler
        logging.root.addHandler(new_handler)

    def _clearHandler(self, typ: str):
        old = self._handlers.get(typ)
        if old:
            logging.root.removeHandler(old)

    def _clearAllHandlers(self):
        for hdlr in logging.root.handlers:
            logging.root.removeHandler(hdlr)

    @staticmethod
    def _addTraceToLogging():
        logging.addLevelName(TRACE_LOG_LEVEL, "TRACE")

        def trace(self, message, *args, **kwargs):
            if self.isEnabledFor(TRACE_LOG_LEVEL):
                self._log(TRACE_LOG_LEVEL, message, args, **kwargs)

        logging.Logger.trace = trace

    @staticmethod
    def _addDisplayToLogging():
        logging.addLevelName(DISPLAY_LOG_LEVEL, "DISPLAY")

        def display(self, message, *args, **kwargs):
            if self.isEnabledFor(DISPLAY_LOG_LEVEL):
                self._log(DISPLAY_LOG_LEVEL, message, args, **kwargs)

        logging.Logger.display = display

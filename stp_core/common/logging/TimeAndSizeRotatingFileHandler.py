import os
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler


class TimeAndSizeRotatingFileHandler(TimedRotatingFileHandler, RotatingFileHandler):

    def __init__(self, filename, when = 'h', interval = 1, backupCount = 0,
                 encoding = None, delay = False, utc = False, atTime = None,
                 maxBytes=0):

        TimedRotatingFileHandler.__init__(self, filename, when, interval,
                                          backupCount, encoding, delay,
                                          utc, atTime)
        self.maxBytes = maxBytes

    def shouldRollover(self, record):
        return bool(TimedRotatingFileHandler.shouldRollover(self, record)) or \
               bool(RotatingFileHandler.shouldRollover(self, record))

    def rotation_filename(self, default_name: str):

        if not os.path.exists(default_name):
            return default_name

        dir = os.path.dirname(default_name)
        defaultFileName = os.path.basename(default_name)
        fileNames = os.listdir(dir)

        maxIndex = -1
        for fileName in fileNames:
            if fileName.startswith(defaultFileName):
                split = fileName.split(".")
                try:
                    index = int(split[-1] if len(split) > 0 else 0)
                except ValueError:
                    index = 0
                if index > maxIndex:
                    maxIndex = index
        return "{}.{}".format(default_name, maxIndex + 1)

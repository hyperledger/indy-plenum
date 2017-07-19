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
                index = self._file_index(fileName)
                if index > maxIndex:
                    maxIndex = index
        return "{}.{}".format(default_name, maxIndex + 1)

    @staticmethod
    def _file_index(file_name):
        split = file_name.split(".")
        try:
            return int(split[-1] if len(split) > 0 else 0)
        except ValueError:
            return 0

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.

        More specific than the earlier method, which just used glob.glob().
        """
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = baseName + "."
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                if self.extMatch.match(suffix):
                    result.append(os.path.join(dirName, fileName))
        if len(result) <= self.backupCount:
            result = []
        else:
            result.sort(key=os.path.getmtime)
            result = result[:len(result) - self.backupCount]
        return result

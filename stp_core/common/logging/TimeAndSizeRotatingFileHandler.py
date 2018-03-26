import os
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler


class TimeAndSizeRotatingFileHandler(TimedRotatingFileHandler, RotatingFileHandler):

    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=False, utc=False, atTime=None,
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
            return int(split[-1])
        except ValueError:
            return 0

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.

        Note: This is copied from `TimedRotatingFileHandler`. The reason for
        copying is to allow sorting in a custom way (by modified time).
        Also minor optimisation to sort only when needed (>self.backupCount)
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
            self._sort_for_removal(result)
            result = result[:len(result) - self.backupCount]
        return result

    @staticmethod
    def _sort_for_removal(result):
        """
        Sort files in the order they should be removed.
        Currently using last modification time but this method can be overridden
        """
        result.sort(key=os.path.getmtime)

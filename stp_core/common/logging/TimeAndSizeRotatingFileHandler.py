import os
import gzip
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler


class TimeAndSizeRotatingFileHandler(TimedRotatingFileHandler, RotatingFileHandler):

    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=False, utc=False, atTime=None,
                 maxBytes=0, compress=False):

        TimedRotatingFileHandler.__init__(self, filename, when, interval,
                                          backupCount, encoding, delay,
                                          utc, atTime)
        self.maxBytes = maxBytes
        self.compress = compress

    def shouldRollover(self, record):
        return bool(TimedRotatingFileHandler.shouldRollover(self, record)) or \
            bool(RotatingFileHandler.shouldRollover(self, record))

    def rotate(self, source, dest):
        source_gz = source.endswith(".gz")
        dest_gz = dest.endswith(".gz")
        if source_gz == dest_gz:
            os.rename(source, dest)
            return
        # Assuming that not source_gz and dest_gz
        with open(source, 'rb') as f_in, gzip.open(dest, 'wb') as f_out:
            f_out.writelines(f_in)
        os.remove(source)

    def rotation_filename(self, default_name: str):
        compressed_name = self._compressed_filename(default_name)
        if not os.path.exists(compressed_name):
            return compressed_name

        dir = os.path.dirname(default_name)
        defaultFileName = os.path.basename(default_name)
        fileNames = os.listdir(dir)

        maxIndex = -1
        for fileName in fileNames:
            if fileName.startswith(defaultFileName):
                index = self._file_index(fileName)
                if index > maxIndex:
                    maxIndex = index
        return self._compressed_filename("{}.{}".format(default_name, maxIndex + 1))

    def _compressed_filename(self, file_name):
        return "{}.gz".format(file_name) if self.compress else file_name

    @staticmethod
    def _file_index(file_name):
        split = file_name.split(".")
        index = split[-1]
        if index == "gz":
            index = split[-2]
        try:
            return int(index)
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
                if suffix.endswith(".gz"):
                    suffix = suffix[:-3]
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

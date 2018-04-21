import os
import gzip
import lzma
from logging import Logger
from datetime import datetime, timedelta
from multiprocessing import Process
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler



class TimeAndSizeRotatingFileHandler(TimedRotatingFileHandler, RotatingFileHandler):

    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=False, utc=False, atTime=None,
                 maxBytes=0, compression=None):

        TimedRotatingFileHandler.__init__(self, filename, when, interval,
                                          backupCount, encoding, delay,
                                          utc, atTime)
        self.maxBytes = maxBytes
        self.compression = compression
        self.compressor = None

    def shouldRollover(self, record):
        return bool(TimedRotatingFileHandler.shouldRollover(self, record)) or \
               bool(RotatingFileHandler.shouldRollover(self, record))

    def rotate(self, source, dest):
        source_compression = self._file_compression(source)
        dest_compression = self._file_compression(dest)
        if source_compression == dest_compression:
            os.rename(source, dest)
            return

        self._finish_compression()
        self.compressor = Process(target=TimeAndSizeRotatingFileHandler._recompress, args=(source, dest))
        self.compressor.start()
        # self._finish_compression()

    def rotation_filename(self, default_name: str):
        self._finish_compression()

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
        return "{}.{}".format(file_name, self.compression) if self.compression else file_name

    def _finish_compression(self):
        if self.compressor is None:
            return

        if not self.compressor.is_alive():
            self.compressor = None
            return

        #logger = Logger()
        now = datetime.now()
        #logger.warning("Log compression in progress while new log needs to be compressed, joining process")
        self.compressor.join()
        delta = datetime.now() - now
        #if delta > timedelta(2):
        #    logger.warning("Waiting for log compression worker took more than 2 seconds")
        self.compressor = None

    @staticmethod
    def _file_compression(file_name):
        if file_name.endswith(".gz"): return "gz"
        if file_name.endswith(".xz"): return "xz"
        return None

    @staticmethod
    def _open_log(file_name, mode):
        compression = TimeAndSizeRotatingFileHandler._file_compression(file_name)
        if compression == "gz": return gzip.open(file_name, mode)
        if compression == "xz": return lzma.open(file_name, mode)
        return open(file_name, mode)

    @staticmethod
    def _recompress(source, dest):
        with TimeAndSizeRotatingFileHandler._open_log(source, 'rb') as f_in, \
                TimeAndSizeRotatingFileHandler._open_log(dest, 'wb') as f_out:
            f_out.write(f_in.read())
        os.remove(source)

    @staticmethod
    def _file_index(file_name):
        split = file_name.split(".")
        index = split[-1]
        if index in ["gz", "xz"]:
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
                if suffix.endswith(".xz"):
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

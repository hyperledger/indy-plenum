import os
import re
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

        log_prefix = os.path.basename(self.baseFilename)
        self.log_pattern = re.compile("^{}(?:|\.(\d+)(?:|\.gz|\.xz))$".format(log_prefix))

        file_indexes = [idx for name, idx in self._log_files()]
        self.max_index = max(file_indexes) if file_indexes else 0

    def shouldRollover(self, record):
        return bool(TimedRotatingFileHandler.shouldRollover(self, record)) or \
               bool(RotatingFileHandler.shouldRollover(self, record))

    def rotate(self, source, dest):
        self.max_index += 1

        source_compression = self._file_compression(source)
        dest_compression = self._file_compression(dest)
        if source_compression == dest_compression:
            os.rename(source, dest)
            return

        tmp_dir, tmp_file = os.path.split(dest)
        tmp_file = self._file_update_compression(tmp_file, source_compression)
        tmp_file = os.path.join(tmp_dir, ".tmp_{}".format(tmp_file))
        os.rename(source, tmp_file)

        self._finish_compression()
        self.compressor = Process(target=TimeAndSizeRotatingFileHandler._recompress, args=(tmp_file, dest))
        self.compressor.start()

    def rotation_filename(self, default_name: str):
        log_name = "{}.{}".format(self.baseFilename, self.max_index + 1)
        log_name = self._file_update_compression(log_name, self.compression)
        return log_name

    def _finish_compression(self):
        if self.compressor is None:
            return

        if not self.compressor.is_alive():
            self.compressor = None
            return

        # logger = Logger()
        now = datetime.now()
        # logger.warning("Log compression in progress while new log needs to be compressed, joining process")
        self.compressor.join()
        delta = datetime.now() - now
        # if delta > timedelta(2):
        #     logger.warning("Waiting for log compression worker took more than 2 seconds")
        self.compressor = None

    def _log_files(self):
        log_dir = os.path.dirname(self.baseFilename)

        def log_info(m):
            idx = int(m.group(1)) if m.group(1) is not None else 0
            return os.path.join(log_dir, m.group(0)), idx

        matches = (self.log_pattern.match(name) for name in os.listdir(log_dir))
        return (log_info(m) for m in matches if m is not None)

    @staticmethod
    def _file_compression(filename):
        if filename.endswith(".gz"): return "gz"
        if filename.endswith(".xz"): return "xz"
        return None

    @staticmethod
    def _file_update_compression(filename, compression):
        if filename.endswith(".gz") or filename.endswith(".xz"):
            filename = filename[:-3]
        if compression is None:
            return filename
        return "{}.{}".format(filename, compression)

    @staticmethod
    def _open_log(filename, mode):
        compression = TimeAndSizeRotatingFileHandler._file_compression(filename)
        if compression == "gz": return gzip.open(filename, mode)
        if compression == "xz": return lzma.open(filename, mode)
        return open(filename, mode)

    @staticmethod
    def _recompress(source, dest):
        with TimeAndSizeRotatingFileHandler._open_log(source, 'rb') as f_in, \
                TimeAndSizeRotatingFileHandler._open_log(dest, 'wb') as f_out:
            f_out.write(f_in.read())
        os.remove(source)

    def getFilesToDelete(self):
        log_files = [(name, idx) for name, idx in self._log_files()]
        if len(log_files) == 0:
            return []

        log_files, log_indexes = zip(*log_files)
        keep_count = self.backupCount

        # This can happen when compression is still in progress
        if max(log_indexes) < self.max_index:
            keep_count -= 1

        if len(log_files) <= keep_count:
            return []

        log_files = list(log_files)
        log_files.sort(key=os.path.getmtime)
        return log_files[:-keep_count]

import os
import re
import gzip
import lzma
import importlib
from time import perf_counter
from multiprocessing import Process
from logging.handlers import RotatingFileHandler


class CompressingFileHandler(RotatingFileHandler):

    def __init__(self, filename, maxBytes=0, backupCount=0, delay=False, compression=None):
        RotatingFileHandler.__init__(self, filename, maxBytes=maxBytes, backupCount=backupCount, delay=delay)

        self.compression = compression
        self.compressor = None

        log_dir, log_name = os.path.split(self.baseFilename)
        self.log_pattern = re.compile("^{}(?:|\.(\d+)(?:|\.gz|\.xz))$".format(log_name))

        file_indexes = [idx for name, idx in self._log_files()]
        self.max_index = max(file_indexes) if file_indexes else 0

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        self._remove_old_files()

        self.max_index += 1
        self._finish_compression()
        uncompressed = self._rotated_uncompressed_filename(self.max_index)
        os.rename(self.baseFilename, uncompressed)

        if self.compression is not None:
            dest = self._rotated_filename(self.max_index)
            self.compressor = Process(target=CompressingFileHandler._recompress, args=(uncompressed, dest))
            self.compressor.start()

        if not self.delay:
            self.stream = self._open()

    def _rotated_uncompressed_filename(self, index):
        return "{}.{}".format(self.baseFilename, index)

    def _rotated_filename(self, index):
        result = self._rotated_uncompressed_filename(index)
        if self.compression is not None:
            result = "{}.{}".format(result, self.compression)
        return result

    def _finish_compression(self):
        if self.compressor is None:
            return

        if not self.compressor.is_alive():
            self.compressor = None
            return

        now = perf_counter()
        self.compressor.join()
        self.compressor = None
        delta = perf_counter() - now
        if delta < 1.0:
            return

        logger = importlib.import_module("stp_core.common.log").getlogger()
        logger.warning("Needed to join log compression process which took {} seconds in main process".format(delta))

    def _log_files(self):
        log_dir = os.path.dirname(self.baseFilename)

        def log_info(m):
            idx = int(m.group(1)) if m.group(1) is not None else 0
            return os.path.join(log_dir, m.group(0)), idx

        matches = (self.log_pattern.match(name) for name in os.listdir(log_dir))
        return (log_info(m) for m in matches if m is not None)

    @staticmethod
    def _open_log(filename, mode):
        if filename.endswith(".gz"):
            return gzip.open(filename, mode)
        if filename.endswith(".xz"):
            return lzma.open(filename, mode)
        return open(filename, mode)

    @staticmethod
    def _recompress(source, dest):
        with CompressingFileHandler._open_log(source, 'rb') as f_in, \
                CompressingFileHandler._open_log(dest, 'wb') as f_out:
            f_out.write(f_in.read())
        os.remove(source)

    def _remove_old_files(self):
        if self.backupCount == 0:
            return

        for file, idx in self._log_files():
            if idx == 0:
                continue
            if idx > self.max_index - self.backupCount + 1:
                continue
            os.remove(file)

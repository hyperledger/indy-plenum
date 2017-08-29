import shutil
import warnings
from tempfile import TemporaryDirectory

# TODO: move it to plenum-util repo


class SafeTemporaryDirectory(TemporaryDirectory):
    """TemporaryDirectory that works on Windows 10
    """

    @classmethod
    def _cleanup(cls, name, warn_message):
        shutil.rmtree(name, ignore_errors=True)
        warnings.warn(warn_message, ResourceWarning)

    def cleanup(self):
        if self._finalizer.detach():
            shutil.rmtree(self.name, ignore_errors=True)

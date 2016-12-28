import shutil
import warnings
import weakref
from tempfile import mkdtemp

class SafeTemporaryDirectory(object):
    """Version of tempfile.TemporaryDirectory that works on Windows 10
    """

    def __init__(self, suffix=None, prefix=None, dir=None):
        self.name = mkdtemp(suffix, prefix, dir)
        self._finalizer = weakref.finalize(
            self, self._cleanup, self.name,
            warn_message="Implicitly cleaning up {!r}".format(self))

    @classmethod
    def _cleanup(cls, name, warn_message):
        shutil.rmtree(name, ignore_errors=True)
        warnings.warn(warn_message, ResourceWarning)


    def __repr__(self):
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def __enter__(self):
        return self.name

    def __exit__(self, exc, value, tb):
        self.cleanup()

    def cleanup(self):
        if self._finalizer.detach():
            shutil.rmtree(self.name, ignore_errors=True)

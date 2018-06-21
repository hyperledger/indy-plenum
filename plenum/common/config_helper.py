import os

from common.exceptions import PlenumValueError, ValueUndefinedError


class PConfigHelper:

    def __init__(self, config, *, chroot=None):
        if config is None:
            raise ValueUndefinedError('config')
        if chroot is not None and not chroot.startswith("/"):
            raise PlenumValueError('chroot', chroot, "starts with '/'")
        self.config = config
        self.chroot = chroot

    def chroot_if_needed(self, path):
        return self._chroot_if_needed(path, self.chroot)

    @property
    def log_dir(self):
        return self.chroot_if_needed(self.config.LOG_DIR)

    @property
    def genesis_dir(self):
        return self.chroot_if_needed(self.config.GENESIS_DIR)

    @property
    def plugins_dir(self):
        return self.chroot_if_needed(self.config.PLUGINS_DIR)

    @property
    def keys_dir(self):
        return self.chroot_if_needed(self.config.KEYS_DIR)

    @property
    def node_info_dir(self):
        return self.chroot_if_needed(self.config.NODE_INFO_DIR)

    @staticmethod
    def _chroot_if_needed(path, chroot):
        result = path
        if chroot is not None and chroot != "/":
            _path = path[1:] if path.startswith("/") else path
            result = os.path.join(chroot, _path)
        return result


class PNodeConfigHelper(PConfigHelper):

    def __init__(self, name: str, config, *, chroot=None):
        if name is None:
            raise ValueUndefinedError('name')
        super().__init__(config, chroot=chroot)
        self.name = name

    @property
    def ledger_dir(self):
        return self.chroot_if_needed(os.path.join(self.config.LEDGER_DIR,
                                                  self.name))

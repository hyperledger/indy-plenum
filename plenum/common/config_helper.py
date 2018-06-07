import os


class PConfigHelper:

    def __init__(self, config, *, chroot=None):
        assert config is not None
        if chroot is not None:
            assert chroot.startswith("/")
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
        assert name is not None
        super().__init__(config, chroot=chroot)
        self.name = name

    @property
    def ledger_dir(self):
        return self.chroot_if_needed(os.path.join(self.config.LEDGER_DIR,
                                                  self.name))

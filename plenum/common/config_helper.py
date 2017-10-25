import os


class PConfigHelper():

    def __init__(self, config, *, chroot="/"):
        assert config is not None
        assert chroot.startswith("/")
        self.config = config
        self.chroot = chroot


    def chroot_if_needed(self, path):
        result = path
        if self.chroot != "/":
            _path = path[1:] if path.startswith("/") else path
            result = os.path.join(self.chroot, _path)
        return result


    @property
    def log_dir(self):
        return self.chroot_if_needed(self.config.LOG_DIR)

    @property
    def genesis_dir(self):
        return self.chroot_if_needed(self.config.GENESIS_DIR)

    @property
    def plugins_dir(self):
        return self.chroot_if_needed(self.config.PLUGINS_DIR)


class PNodeConfigHelper(PConfigHelper):

    def __init__(self, name: str, config, *, chroot='/'):
        assert name is not None
        super().__init__(config, chroot=chroot)
        self.name = name

    @property
    def ledger_dir(self):
        return self.chroot_if_needed(os.path.join(self.config.LEDGER_DIR, 'data', self.name))

    @property
    def keys_dir(self):
        return self.chroot_if_needed(os.path.join(self.config.KEYS_DIR, self.name))


class PClientConfigHelper(PConfigHelper):
    @property
    def wallet_dir(self):
        return os.path.join(self.config.WALLET_DIR)
